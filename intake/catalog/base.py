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
from .utils import flatten, reload_on_change
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
        # Allow the subclass to have set self._entries to some other dict-like
        # object *before* this is run so that the last thing to happen is
        # force_reload().
        if getattr(self, '_entries', None) is None:
            self._entries = {}
        self.force_reload()

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
        try:
            return self._entries[key]
        except KeyError:
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


class RemoteCatalog(Catalog):
    """The state of a remote Intake server"""
    def __init__(self, url, http_args={}, page_size=100, **kwargs):
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
            Default is 100.
        kwargs: may include catalog name, metadata, source ID (if known) and
            auth instance.
        """
        self.http_args = http_args
        self.http_args.update(kwargs.get('storage_options', {}))
        self.http_args['headers'] = self.http_args.get('headers', {})
        self.page_size = page_size
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

        def fetch_page(page_number):
            logger.debug("Request page %d of entries", page_number)
            params = {'page[number]': page_number,
                      'page[size]': self.page_size}
            response = requests.get(self.info_url, params=params,
                                    **self._get_http_args())
            if response.status_code != 200:
                raise Exception('%s: status code %d' % (response.url,
                                                        response.status_code))
            info = msgpack.unpackb(response.content, encoding='utf-8')
            page = {source['name']: RemoteCatalogEntry(
                url=self.source_url,
                getenv=self.getenv,
                getshell=self.getshell,
                auth=self.auth,
                http_args=self.http_args, **source)
                for source in info['sources']}
            return page

        def fetch_by_name(name):
            logger.debug("Requesting info about entry named '%s'", name)
            params = {'name': name}
            response = requests.get(self.source_url, params=params,
                                    **self._get_http_args())
            if response.status_code == 404:
                raise KeyError(name)
            if response.status_code != 200:
                raise Exception('%s: status code %d' % (response.url,
                                                        response.status_code))
            info = msgpack.unpackb(response.content, encoding='utf-8')
            return RemoteCatalogEntry(
                url=self.source_url,
                getenv=self.getenv,
                getshell=self.getshell,
                auth=self.auth,
                http_args=self.http_args, **info['source'])

        def server_can_paginate():
            return self._server_can_paginate

        class Entries:
            """Mock enough of the dict interface.

            This fetches pages of entries from the server during iteration and
            caches them. On __getitem__ it fetches the sepcific entry from the
            server.
            """
            # This has PY3-style lazy methods (keys, values, items). Since it's
            # internal we should not need the PY2-only iter* variants.
            def __init__(self):
                self._page_cache = collections.OrderedDict()
                # Put lookups that were due to __getitem__ in a separate cache
                # so that iteration reflects the server's order, not an
                # arbitrary cache order.
                self._direct_lookup_cache = {}
                self._highest_page_fetched = 0

            def __iter__(self):
                for key in self.keys():
                    yield key

            def items(self):
                for item in six.iteritems(self._page_cache):
                    yield item
                if not server_can_paginate():
                    # We are talking to an older server, before pagination
                    # support was added. It would have already sent us all its
                    # entries, so we are done.
                    return
                # Fetch more entries from the server.
                while True:
                    page = fetch_page(self._highest_page_fetched + 1)
                    self._highest_page_fetched += 1
                    self._page_cache.update(page)
                    if not page:
                        # The server has no more entries (but it might next
                        # time we check).
                        break
                    for item in six.iteritems(page):
                        yield item

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
                        source = fetch_by_name(key)
                        self._direct_lookup_cache[key] = source
                        return source

        self._entries = Entries()
        self._server_can_paginate = True
        super(RemoteCatalog, self).__init__(self, **kwargs)

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

        # Just fetch the metadata.
        params = {'page[number]': 1, 'page[size]': 0}
        response = requests.get(self.info_url, params=params,
                                **self._get_http_args())
        if response.status_code != 200:
            raise Exception('%s: status code %d' % (response.url,
                                                    response.status_code))
        info = msgpack.unpackb(response.content, encoding='utf-8')
        self.metadata = info['metadata']
        # If the server respects the pagination parameters, info['sources']
        # should be empty. But if we have an old server, it will contain all
        # the entries and we should cache them now.
        if info['sources']:
            self._server_can_paginate = False
            self._entries._page_cache.update(
                {source['name']: RemoteCatalogEntry(
                    url=self.source_url,
                    getenv=self.getenv,
                    getshell=self.getshell,
                    auth=self.auth,
                    http_args=self.http_args, **source)
                 for source in info['sources']})
