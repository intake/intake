#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import collections
import copy
import keyword
import logging
import posixpath
import re
import six
import time
import warnings

import msgpack
import requests
from requests.compat import urljoin, urlparse

from ..auth.base import BaseClientAuth
from .remote import RemoteCatalogEntry
from .utils import flatten, reload_on_change, RemoteCatalogError
from ..source.base import DataSource
from ..compat import unpack_kwargs
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

    def __init__(self, *args, name=None, metadata=None, auth=None, ttl=1,
                 getenv=True, getshell=True, persist_mode='default',
                 storage_options=None):
        """
        Parameters
        ----------
        args : str or list(str)
            A single URI or list of URIs.
        name : str, optional
            Unique identifier for catalog. This is primarily useful when
            manually constructing a catalog. Defaults to None.
        metadata: dict
            Additional information about this data
        auth : BaseClientAuth or None
            Default, None, falls back to BaseClientAuth.
        ttl : float, optional
            Lifespan (time to live) of cached modification time. Units are in
            seconds. Defaults to 1.
        getenv: bool
            Can parameter default fields take values from the environment
        getshell: bool
            Can parameter default fields run shell commands
        persist_mode: ['always', 'default', 'never']
            Defines the use of persisted sources: if 'always', will use a
            persisted version of a data source, if it exists, if 'never' will
            always use the original source. If 'default', persisted sources
            will be used if they have not expired, and re-persisted and used
            if they have.
        storage_options : dict
            If using a URL beginning with 'intake://' (remote Intake server),
            parameters to pass to requests when issuing http commands; otherwise
            parameters to pass to remote backend file-system. Ignored for
            normal local files.
        """
        super(Catalog, self).__init__()
        self.name = name
        self.metadata = metadata or {}
        self.ttl = ttl
        self.getenv = getenv
        self.getshell = getshell
        self.storage_options = storage_options
        if persist_mode not in ['always', 'never', 'default']:
            # should be True, False, None ?
            raise ValueError('Persist mode (%s) not understood' % persist_mode)
        self.pmode = persist_mode

        if auth is None:
            auth = BaseClientAuth()
        self.auth = auth

        if all(isinstance(a, (tuple, list)) for a in args):
            args = list(flatten(args))
        if len(args) == 1:
            args = args[0]
        self.args = args
        self.updated = time.time()
        self._entries = self._make_entries_container()
        self.force_reload()

    @property
    def kwargs(self):
        return dict(name=self.name, ttl=self.ttl)

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

    @reload_on_change
    def search(self, text, depth=2):
        words = text.lower().split()
        cat = Catalog(name=self.name + "_search",
                      ttl=self.ttl,
                      getenv=self.getenv,
                      getshell=self.getshell,
                      auth=self.auth,
                      metadata=(self.metadata or {}).copy(),
                      storage_options=self.storage_options)
        cat.metadata['search'] = {'text': text, 'upstream': self.name}
        cat._entries = {k: v for k, v in self.walk(depth=depth).items()
                        if any(word in str(v.describe().values()).lower()
                               for word in words)}
        cat.cat = self
        for e in cat._entries.values():
            e._catalog = cat
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
        entry = self._entries[name]
        entry._catalog = self
        entry._pmode = self.pmode
        return entry

    @reload_on_change
    def _get_entries(self):
        return self._entries

    def __iter__(self):
        """Return an iterator over catalog entries."""
        return iter(self._get_entries())

    def __contains__(self, key):
        # Avoid iterating through all entries.
        return key in self._get_entries()  # triggers reload_on_change

    def __dir__(self):
        # Include tab-completable entries and normal attributes.
        return (
            [entry for entry in self if
             re.match("[_A-Za-z][_a-zA-Z0-9]*$", entry)  # valid Python identifer
             and not keyword.iskeyword(entry)]  # not a Python keyword
            + list(self.__dict__.keys()))

    def _ipython_key_completions_(self):
        return list(self)

    def __repr__(self):
        return "<Intake catalog: %s>" % self.name

    def __getattr__(self, item):
        if not item.startswith('_'):
            # Fall back to __getitem__.
            try:
                return self[item]  # triggers reload_on_change
            except KeyError:
                raise AttributeError(item)
        raise AttributeError(item)

    def __getitem__(self, key):
        """Return a catalog entry by name.

        Can also use attribute syntax, like ``cat.entry_name``, or
        item lookup cat['non-python name']. This enables walking through
        nested directories with cat.name1.name2, cat['name1.name2'] *or*
        cat['name1', 'name2']
        """
        if not isinstance(key, list) and key in self._get_entries():
            # triggers reload_on_change
            e = self._entries[key]
            e._catalog = self
            e._pmode = self.pmode
            return e
        if isinstance(key, str) and '.' in key:
            key = key.split('.')
        if isinstance(key, (tuple, list)):
            if len(key) > 1 and key[0] in self._entries:
                out = self._entries[key[0]].__getitem__(key[1:])
                return out
            if len(key) == 1:
                return self[key[0]]
        else:
            raise KeyError(key)

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
        # True if all pages are cached locally
        self.complete = self._catalog.page_size is None

    def reset(self):
        "Clear caches to force a reload."
        self._page_cache.clear()
        self._direct_lookup_cache.clear()
        self._page_offset = 0
        self.complete = self._catalog.page_size is None

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
                self.complete = True
                break

    def cached_items(self):
        """
        Iterate over items that are already cached. Perform no requests.
        """
        for item in six.iteritems(self._page_cache):
            yield item
        for item in six.iteritems(self._direct_lookup_cache):
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
                source = self._catalog.fetch_by_name(key)
                self._direct_lookup_cache[key] = source
                return source


class RemoteCatalog(Catalog):
    """The state of a remote Intake server"""
    name = 'intake_remote'

    def __init__(self, url, http_args=None, page_size=None,
                 name=None, source_id=None, metadata=None, auth=None, ttl=1,
                 getenv=True, getshell=True,
                 storage_options=None, parameters=None):
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
        name : str, optional
            Unique identifier for catalog. This is primarily useful when
            manually constructing a catalog. Defaults to None.
        source_id : str, optional
            Emphemeral unique ID generated by the server, if known.
        metadata: dict
            Additional information about this data
        auth : BaseClientAuth or None
            Default, None, falls back to BaseClientAuth.
        ttl : float, optional
            Lifespan (time to live) of cached modification time. Units are in
            seconds. Defaults to 1.
        getenv: bool
            Can parameter default fields take values from the environment
        getshell: bool
            Can parameter default fields run shell commands
        storage_options : dict
            If using a URL beginning with 'intake://' (remote Intake server),
            parameters to pass to requests when issuing http commands; otherwise
            parameters to pass to remote backend file-system. Ignored for
            normal local files.
        parameters: dict
            To pass to the server when it instantiates the data source
        """
        if http_args is None:
            http_args = {}
        else:
            # Make a deep copy to avoid mutating input.
            http_args = copy.deepcopy(http_args)
        secure = http_args.pop('ssl', False)
        scheme = 'https' if secure else 'http'
        url = url.replace('intake', scheme)
        if not url.endswith('/'):
            url = url + '/'
        self.url = url
        self.info_url = urljoin(url, 'v1/info')
        self.source_url = urljoin(url, 'v1/source')
        self.http_args = http_args
        self.http_args.update(storage_options or {})
        self.http_args['headers'] = self.http_args.get('headers', {})
        self._page_size = page_size
        self._source_id = source_id
        self._parameters = parameters
        self._len = None
        if self._source_id is None:
            name = urlparse(url).netloc.replace(
                '.', '_').replace(':', '_')
        super(RemoteCatalog, self).__init__(
            name=name, metadata=name, auth=auth, ttl=ttl, getenv=getenv,
            getshell=getshell, storage_options=storage_options)

    def _make_entries_container(self):
        return Entries(self)

    def __dir__(self):
        # Include (cached) tab-completable entries and normal attributes.
        return (
            [key for key in self._ipython_key_completions_() if
             re.match("[_A-Za-z][_a-zA-Z0-9]*$", key)  # valid Python identifier
             and not keyword.iskeyword(key)]  # not a Python keyword
            + list(self.__dict__.keys()))

    def _ipython_key_completions_(self):
        if not self._entries.complete:
            # Ensure that at least one page of data has been loaded so that
            # *some* entries are included.
            next(iter(self))
        if not self._entries.complete:
            warnings.warn(
                "Tab-complete and dir() on RemoteCatalog may include only a "
                "subset of the available entries.")
        # Loop through the cached entries, but do not trigger iteration over
        # the full set.
        # Intentionally access _entries directly to avoid paying for a reload.
        return [key for key, _ in self._entries.cached_items()]

    @property
    def page_size(self):
        return self._page_size

    def fetch_page(self, page_offset):
        logger.debug("Request page entries %d-%d",
                     page_offset, page_offset + self._page_size)
        params = {'page_offset': page_offset,
                  'page_size': self._page_size}
        http_args = self._get_http_args(params)
        response = requests.get(self.info_url, **http_args)
        # Produce a chained exception with both the underlying HTTPError
        # and our own more direct context.
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            six.raise_from(RemoteCatalogError(
                "Failed to fetch page of entries {}-{}."
                "".format(page_offset, page_offset + self._page_size)), err)
        info = msgpack.unpackb(response.content, **unpack_kwargs)
        page = {}
        for source in info['sources']:
            user_parameters = source.get('user_parameters', [])
            # TODO Do something with self._parameters.
            page[source['name']] = RemoteCatalogEntry(
                url=self.url,
                getenv=self.getenv,
                getshell=self.getshell,
                auth=self.auth,
                http_args=self.http_args,
                page_size=self._page_size,
                # user_parameters=user_parameters,
                **source)
        return page

    def fetch_by_name(self, name):
        logger.debug("Requesting info about entry named '%s'", name)
        params = {'name': name}
        http_args = self._get_http_args(params)
        response = requests.get(self.source_url, **http_args)
        if response.status_code == 404:
            raise KeyError(name)
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            six.raise_from(RemoteCatalogError(
                "Failed to fetch entry {!r}.".format(name)), err)
        info = msgpack.unpackb(response.content, **unpack_kwargs)
        return RemoteCatalogEntry(
            url=self.url,
            getenv=self.getenv,
            getshell=self.getshell,
            auth=self.auth,
            http_args=self.http_args,
            page_size=self._page_size,
            **info['source'])

    def _get_http_args(self, params):
        """
        Return a copy of the http_args

        Adds auth headers and 'source_id', merges in params.
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

        # Merge in any params specified by the caller.
        merged_params = http_args.get('params', {})
        merged_params.update(params)
        http_args['params'] = merged_params
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
        http_args = self._get_http_args(params)
        response = requests.get(self.info_url, **http_args)
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            six.raise_from(RemoteCatalogError(
                "Failed to fetch metadata."), err)
        info = msgpack.unpackb(response.content, **unpack_kwargs)
        self.metadata = info['metadata']
        # The intake server now always provides a length, but the server may be
        # running an older version of intake.
        self._len = info.get('length')
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
                    url=self.url,
                    getenv=self.getenv,
                    getshell=self.getshell,
                    auth=self.auth,
                    http_args=self.http_args, **source)
                 for source in info['sources']})

    def search(self, *args, **kwargs):
        request = {'action': 'search', 'query': (args, kwargs),
                   'source_id': self._source_id}
        response = requests.post(
            url=self.source_url, **self._get_http_args({}),
            data=msgpack.packb(request, use_bin_type=True))
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            six.raise_from(RemoteCatalogError("Failed search query."), err)
        source = msgpack.unpackb(response.content, **unpack_kwargs)
        source_id = source['source_id']
        cat = RemoteCatalog(
            url=self.url,
            http_args=self.http_args,
            source_id=source_id,
            name="")
        cat.cat = self
        return cat

    def __len__(self):
        if self._len is None:
            # The server is running an old version of intake and did not
            # provide a length, so we have no choice but to do this the
            # expensive way.
            return sum(1 for _ in self)
        else:
            return self._len

    @staticmethod
    def _persist(source, path, **kwargs):
        from intake.catalog.local import YAMLFileCatalog
        from dask.bytes.core import open_files
        import yaml
        out = {}
        for name in source:
            entry = source[name]
            out[name] = entry.__getstate__()
        fn = posixpath.join(path, 'cat.yaml')
        with open_files([fn], 'wt')[0] as f:
            yaml.dump({'sources': out}, f)
        return YAMLFileCatalog(fn)
