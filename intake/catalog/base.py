import collections
import logging
import six
import time

import msgpack
import requests
from requests.compat import urljoin, urlparse

from ..auth.base import BaseClientAuth
from .entry import CatalogEntry
from .remote import RemoteCatalogEntry
from .utils import flatten, reload_on_change, RemoteCatalogError
from ..source.base import DataSource
logger = logging.getLogger('intake')


class Catalog(DataSource):
    """Manages a hierarchy of data sources as a collective unit.

    A catalog is a set of available data sources for an individual
    entity (remote server, local  file, or a local
    directory of files). This can be expanded to include a
    collection of subcatalogs, which are then managed as a single unit.

    A catalog is created with a single URI or a collection of URIs. A URI can
    either be a URL or a file path.

    Each catalog in the hierarchy is responsible for caching the most recent
    refresh time to prevent overeager queries.

    Attributes
    ----------
    metadata : dict
        Arbitrary information to carry along with the data source specs.
    """
    # emulate a DataSource
    container = 'catalog'
    name = 'catalog'

    def __init__(self, *args, **kwargs):
        """
        Parameters
        ----------
        args : str or list(str)
            A single URI or list of URIs.
        name : str, optional
            Unique identifier for catalog. This is primarily useful when
            manually constructing a catalog. Defaults to None.
        ttl : float, optional
            Lifespan (time to live) of cached modification time. Units are in
            seconds. Defaults to 1.
        storage_options : dict
            If using a URL beginning with 'intake://' (remote Intake server),
            parameters to pass to requests when issuing http commands; otherwise
            parameters to pass to remote backend file-system. Ignored for
            normal local files.
        """
        super(Catalog, self).__init__()
        self.name = kwargs.get('name', None)
        self.ttl = kwargs.get('ttl', 1)
        self.getenv = kwargs.pop('getenv', True)
        self.getshell = kwargs.pop('getshell', True)
        self.auth = kwargs.pop('auth', None)
        self.metadata = kwargs.pop('metadata', None)
        self.storage_options = kwargs.pop('storage_options', {})
        self.kwargs = kwargs

        if self.auth is None:
            self.auth = BaseClientAuth()

        if all(isinstance(a, (tuple, list)) for a in args):
            args = list(flatten(args))
        if len(args) == 1:
            args = args[0]
        self.args = args
        self.updated = time.time()
        self._entries = self._make_entries_container()
        self.force_reload()

    def _make_entries_container(self):
        """Subclasses may override this to return some other dict-like.

        See RemoteCatalog below for the motivating example for this hook.
        """
        return {}

    def _load(self):
        """Override this: load catalog entries"""
        pass

    def force_reload(self):
        """Imperative reload data now"""
        self._load()
        self.updated = time.time()

    def reload(self):
        """Reload catalog if sufficient time has passed"""
        if time.time() - self.updated > self.ttl:
            self.force_reload()

    @property
    def version(self):
        # default version for pre-v1 files
        return self.metadata.get('version', 1)

    def search(self, text, depth=2):
        words = text.lower().split()
        cat = Catalog(name=self.name + "_search",
                      getenv=self.getenv,
                      getshell=self.getshell,
                      auth=self.auth,
                      metadata=(self.metadata or {}).copy(),
                      storage_options=self.storage_options)
        cat.metadata['search'] = {'text': text, 'upstream': self.name}
        cat._entries = {k: v for k, v in self.walk(depth=depth).items()
                        if any(word in str(v.describe().values()).lower()
                               for word in words)}
        return cat

    @reload_on_change
    def walk(self, sofar=None, prefix=None, depth=2):
        """Get all entries in this catalog and sub-catalogs

        Parameters
        ----------
        sofar: dict or None
            Within recursion, use this dict for output
        prefix: list of str or None
            Names of levels already visited
        depth: int
            Number of levels to descend; needed to truncate circular references
            and for cleaner output

        Returns
        -------
        Dict where the keys are the entry names in dotted syntax, and the
        values are entry instances.
        """
        out = sofar if sofar is not None else {}
        prefix = [] if prefix is None else prefix
        for name, item in self._entries.items():
            if item._container == 'catalog' and depth > 1:
                # recurse with default open parameters
                try:
                    item().walk(out, prefix + [name], depth-1)
                except Exception as e:
                    print(e)
                    pass  # ignore inability to descend
            n = '.'.join(prefix + [name])
            out[n] = item
        return out

    @reload_on_change
    def _get_entry(self, name):
        return self._entries[name]

    @reload_on_change
    def _get_entries(self):
        return self._entries

    def __iter__(self):
        """Return an iterator over catalog entries."""
        return iter(self._get_entries())

    def __contains__(self, key):
        # Avoid iterating through all entries.
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __dir__(self):
        return list(self)

    def __repr__(self):
        return "<Intake catalog: %s>" % self.name

    def __getattr__(self, item):
        if not item.startswith('_'):
            return self._get_entry(item)

    def __getitem__(self, key):
        """Return a catalog entry by name.
        
        Can also use attribute syntax, like ``cat.entry_name``.
        """
        if key in self._entries:
            return self._entries[key]
        out = self
        for k in key.split('.'):
            if isinstance(out, CatalogEntry):
                out = out()  # default parameters
            if not isinstance(out, Catalog):
                raise ValueError("Attempt to recurse into non-catalog")
            out = getattr(out, k)
        return out

    def discover(self):
        return {"container": 'catalog', 'shape': None,
                'dtype': None, 'datashape': None, 'metadata': self.metadata}

    def _close(self):
        # TODO: maybe close all entries?
        pass

class Entries(dict):
    """Fetches entries from server on item lookup and iteration.

    This fetches pages of entries from the server during iteration and
    caches them. On __getitem__ it fetches the sepcific entry from the
    server.
    """
    # This has PY3-style lazy methods (keys, values, items). Since it's
    # internal we should not need the PY2-only iter* variants.
    def __init__(self, catalog):
        self._catalog = catalog
        self._page_cache = collections.OrderedDict()
        # Put lookups that were due to __getitem__ in a separate cache
        # so that iteration reflects the server's order, not an
        # arbitrary cache order.
        self._direct_lookup_cache = {}
        self._page_offset = 0

    def reset(self):
        "Clear caches to force a reload."
        self._page_cache.clear()
        self._direct_lookup_cache.clear()
        self._page_offset = 0

    def __iter__(self):
        for key in self.keys():
            yield key

    def __contains__(self, key):
        # Avoid iterating through all entries.
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def items(self):
        for item in six.iteritems(self._page_cache):
            yield item
        if self._catalog.page_size is None:
            # We are not paginating, either because the user set page_size=None
            # or the server is a version of intake before pagination parameters
            # were added.
            return
        # Fetch more entries from the server.
        while True:
            page = self._catalog.fetch_page(self._page_offset)
            self._page_cache.update(page)
            self._page_offset += len(page)
            for item in six.iteritems(page):
                yield item
            if len(page) < self._catalog.page_size:
                # Partial or empty page.
                # We are done until the next call to items(), when we
                # will resume at the offset where we left off.
                break

    def keys(self):
        for key, value in self.items():
            yield key

    def values(self):
        for key, value in self.items():
            yield value

    def __getitem__(self, key):
        try:
            return self._direct_lookup_cache[key]
        except KeyError:
            try:
                return self._page_cache[key]
            except KeyError:
                source = self._catalog.fetch_by_name(key)
                self._direct_lookup_cache[key] = source
                return source

class RemoteCatalog(Catalog):
    """The state of a remote Intake server"""
    def __init__(self, url, http_args={}, page_size=None, **kwargs):
        """Connect to remote Intake Server as a catalog

        Parameters
        ----------
        url: str
            Address of the server, e.g., "intake://localhost:5000".
        http_args: dict
            Arguments to add to HTTP calls, including "ssl" (True/False) for
            secure connections.
        page_size : int, optional
            The number of entries fetched at a time during iteration.
            Default is None (no pagination; fetch all entries in bulk).
        kwargs: may include catalog name, metadata, source ID (if known) and
            auth instance.
        """
        self.http_args = http_args
        self.http_args.update(kwargs.get('storage_options', {}))
        self.http_args['headers'] = self.http_args.get('headers', {})
        self._page_size = page_size
        self._source_id = kwargs.get('source_id', None)
        if self._source_id is None:
            secure = http_args.pop('ssl', False)
            scheme = 'https' if secure else 'http'
            base_url = url.replace('intake', scheme) + '/'
            self.info_url = urljoin(base_url, 'v1/info')
            self.source_url = urljoin(base_url, 'v1/source')
            self.name = urlparse(base_url).netloc.replace(
                '.', '_').replace(':', '_')
        else:
            self.name = kwargs['name']
            self.source_url = url
            self.info_url = url.replace('v1/source', 'v1/info')
        self.auth = kwargs.get('auth', None)  # instance of BaseClientAuth
        super(RemoteCatalog, self).__init__(self, **kwargs)

    def _make_entries_container(self):
        return Entries(self)

    @property
    def page_size(self):
        return self._page_size

    def fetch_page(self, page_offset):
        logger.debug("Request page entries %d-%d",
                     page_offset, page_offset + self._page_size)
        params = {'page_offset': page_offset,
                  'page_size': self._page_size}
        response = requests.get(self.info_url, params=params,
                                **self._get_http_args())
        # Produce a chained exception with both the underlying HTTPError
        # and our own more direct context.
        try:
            response.raise_for_status()
        except requests.HTTPError:
            raise RemoteCatalogError(
                "Failed to fetch page of entries {}-{}."
                "".format(page_offset, page_offset + self._page_size))
        info = msgpack.unpackb(response.content, encoding='utf-8')
        page = {source['name']: RemoteCatalogEntry(
            url=self.source_url,
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            http_args=self.http_args,
            page_size=self._page_size,
            **source)
            for source in info['sources']}
        return page

    def fetch_by_name(self, name):
        logger.debug("Requesting info about entry named '%s'", name)
        params = {'name': name}
        response = requests.get(self.source_url, params=params,
                                **self._get_http_args())
        if response.status_code == 404:
            raise KeyError(name)
        try:
            response.raise_for_status()
        except requests.HTTPError:
            raise RemoteCatalogError(
                "Failed to fetch entry {!r}.".format(name))
        info = msgpack.unpackb(response.content, encoding='utf-8')
        return RemoteCatalogEntry(
            url=self.source_url,
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            http_args=self.http_args,
            page_size=self._page_size,
            **info['source'])

    def _get_http_args(self):
        """
        Return a copy of the http_args with auth headers and 'source_id' added.
        """
        # Add the auth headers to any other headers
        headers = self.http_args.get('headers', {})
        if self.auth is not None:
            auth_headers = self.auth.get_headers()
            headers.update(auth_headers)

        # build new http args with these headers
        http_args = self.http_args.copy()
        if self._source_id is not None:
            headers['source_id'] = self._source_id
        http_args['headers'] = headers
        return http_args

    def _load(self):
        """Fetch metadata from remote. Entries are fetched lazily."""
        # This will not immediately fetch any sources (entries). It will lazily
        # fetch sources from the server in paginated blocks when this Catalog
        # is iterated over. It will fetch specific sources when they are
        # accessed in this Catalog via __getitem__.

        if self.page_size is None:
            # Fetch all source info.
            params = {}
        else:
            # Just fetch the metadata now; fetch source info later in pages.
            params = {'page_offset': 0, 'page_size': 0}
        response = requests.get(self.info_url, params=params,
                                **self._get_http_args())
        try:
            response.raise_for_status()
        except requests.HTTPError:
            raise RemoteCatalogError(
                "Failed to fetch metadata.")
        info = msgpack.unpackb(response.content, encoding='utf-8')
        self.metadata = info['metadata']
        self._entries.reset()
        # If we are paginating (page_size is not None) and the server we are
        # working with is new enough to support pagination, info['sources']
        # should be empty. If either of those things is not true,
        # info['sources'] will contain all the entries and we should cache them
        # now.
        if info['sources']:
            # Signal that we are not paginating, even if we were asked to.
            self._page_size = None
            self._entries._page_cache.update(
                {source['name']: RemoteCatalogEntry(
                    url=self.source_url,
                    getenv=self.getenv,
                    getshell=self.getshell,
                    auth=self.auth,
                    http_args=self.http_args, **source)
                 for source in info['sources']})
