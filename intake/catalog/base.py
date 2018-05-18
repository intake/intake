import logging
import time

import msgpack
import requests
from requests.compat import urljoin, urlparse
from dask.bytes import open_files

from ..auth.base import BaseClientAuth
from .local import CatalogConfig
from .remote import RemoteCatalogEntry
from .utils import clamp, flatten, reload_on_change, make_prefix_tree
logger = logging.getLogger('intake')


class State(object):
    """Base class representing the state of a catalog source"""

    def __init__(self, name, observable, ttl, getenv=True, getshell=True):
        self.name = name
        self.observable = observable
        self.ttl = clamp(ttl)
        self._modification_time = 0
        self._last_updated = 0
        self.getenv = getenv
        self.getshell = getshell
        self.metadata = {}

    def refresh(self):
        return None, {}, {}, []

    def update_modification_time(self, value):
        now = time.time()
        if now - self._last_updated > self.ttl:
            updated = value > self._modification_time
            self._modification_time = value
            self._last_updated = now
            return updated
        return False

    def changed(self):
        return self.update_modification_time(time.time())


class DirectoryState(State):
    """The state of a directory of catalog files"""

    def __init__(self, name, observable, ttl, getenv=True, getshell=True,
                 storage_options=None):
        super(DirectoryState, self).__init__(name, observable, ttl,
                                             getenv=getenv, getshell=getshell)
        self.catalogs = []
        self.storage_options = storage_options
        self._last_files = []
        self.metadata = {}

    def refresh(self):
        catalogs = []
        self.metadata.clear()
        self._last_files = []

        fns = (open_files(self.observable + '/*.yaml') +
               open_files(self.observable + '/*.yml'))
        for f in fns:
            try:
                self._last_files.append(f.path)
                catalogs.append(Catalog(f))
                self.metadata[f.path] = catalogs[-1].metadata
            except Exception as e:
                logger.warning("%s: %s" % (str(e), f))

        self.catalogs = catalogs
        children = {catalog.name: catalog for catalog in self.catalogs}

        return self.name, children, {}, []

    def changed(self):
        fns = (open_files(self.observable + '/*.yaml') +
               open_files(self.observable + '/*.yml'))

        modified = set(fn.path for fn in fns) != set(self._last_files)
        if modified:
            self.refresh()
        return any([modified] + [catalog.changed for catalog in self.catalogs])


class RemoteState(State):
    """The state of a remote Intake server"""

    def __init__(self, name, observable, ttl, auth, getenv=True, getshell=True,
                 http_args=None):
        super(RemoteState, self).__init__(name, observable, ttl,
                                          getenv=getenv, getshell=getshell)
        self.http_args = http_args or {}
        secure = http_args.pop('ssl', False)
        scheme = 'https' if secure else 'http'
        self.base_url = observable.replace('intake', scheme) + '/'
        self.info_url = urljoin(self.base_url, 'v1/info')
        self.source_url = urljoin(self.base_url, 'v1/source')
        self.auth = auth # instance of BaseClientAuth
        self.metadata = {}

    def refresh(self):
        name = urlparse(self.observable).netloc.replace(
            '.', '_').replace(':', '_')

        # Add the auth headers to any other headers
        auth_headers = self.auth.get_headers()
        headers = self.http_args.get('headers', {})
        headers.update(auth_headers)

        # build new http args with these headers
        http_args = self.http_args.copy()
        http_args['headers'] = headers
        response = requests.get(self.info_url, **http_args)
        if response.status_code != 200:
            raise Exception('%s: status code %d' % (response.url,
                                                    response.status_code))
        info = msgpack.unpackb(response.content, encoding='utf-8')
        self.metadata = info['metadata']

        entries = {s['name']: RemoteCatalogEntry(url=self.source_url,
                                                 getenv=self.getenv,
                                                 getshell=self.getshell,
                                                 auth=self.auth,
                                                 http_args=self.http_args, **s)
                   for s in info['sources']}

        return name, {}, entries, []


class LocalState(State):
    """The state of a catalog file on the local filesystem"""

    def __init__(self, name, observable, ttl, getenv=True, getshell=True,
                 storage_options=None):
        self.storage_options = storage_options
        super(LocalState, self).__init__(name, observable, ttl,
                                         getenv=getenv, getshell=getshell)
        self.token = ''
        self.metadata = {}

    def refresh(self):
        cfg = CatalogConfig(self.observable, getenv=self.getenv,
                            getshell=self.getshell,
                            storage_options=self.storage_options)
        self.token = cfg.token
        self.metadata = cfg.metadata
        return cfg.name, {}, cfg.entries, cfg.plugins

    def changed(self):
        token = CatalogConfig(self.observable, getenv=False, getshell=False,
                              storage_options=self.storage_options).token
        return token != self.token


class CollectionState(State):
    """The state of a collection of other states"""

    def __init__(self, name, observable, ttl, getenv=True, getshell=True,
                 storage_options=None):
        super(CollectionState, self).__init__(name, observable, ttl)
        # This name is a workaround to deal with issue that will be
        # solved in another PR
        self.catalogs = [Catalog(uri, name='cat%d' % i, getenv=getenv,
                                 getshell=getshell,
                                 storage_options=storage_options)
                         for i, uri in enumerate(self.observable)]
        self.metadata = {uri: c.metadata for uri, c in zip(self.observable,
                                                           self.catalogs)}

    def refresh(self):
        for catalog in self.catalogs:
            catalog.reload()
        name = None
        children = {catalog.name: catalog for catalog in self.catalogs}
        return name, children, {}, []

    def changed(self):
        return any([catalog.changed for catalog in self.catalogs])


def create_state(name, observable, ttl, auth, getenv=True, getshell=True,
                 storage_options=None):
    if observable is None or observable == []:
        return State(name, observable, ttl)
    if isinstance(observable, list):
        if len(observable) > 1:
            return CollectionState(name, observable, ttl, getenv=getenv,
                                   getshell=getshell,
                                   storage_options=storage_options)
        if len(observable) == 0:
            return Catalog()
        observable = observable[0]
    if isinstance(observable, str):
        if observable.startswith('intake://'):
            return RemoteState(name, observable, ttl,
                               auth=auth,
                               getenv=getenv,
                               getshell=getshell, http_args=storage_options)
        elif observable.endswith('.yml') or observable.endswith('.yaml'):
            return LocalState(name, observable, ttl, getenv=getenv,
                              getshell=getshell,
                              storage_options=storage_options)
        else:
            return DirectoryState(name, observable, ttl, getenv=getenv,
                                  getshell=getshell,
                                  storage_options=storage_options)
    else:
        # try file-like
        return LocalState(name, observable, ttl, getenv=getenv,
                          getshell=getshell)


class Catalog(object):
    """Manages a hierarchy of data sources and plugins as a collective unit.

    A catalog is a set of available data sources and plugins for an individual
    observed entity (remote server, local configuration file, or a local
    directory of configuration files). This can be expanded to include a
    collection of subcatalogs, which are then managed as a single unit.

    A catalog is created with a single URI or a collection of URIs. A URI can
    either be a URL or a file path.

    Each catalog in the hierarchy is responsible for caching the most recent
    modification time of the respective observed entity to prevent overeager
    queries.

    Attributes
    ----------
    metadata : dict
        Dictionary loaded from ``metadata`` section of catalog file.
    """

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
        name = kwargs.get('name', None)
        ttl = kwargs.get('ttl', 1)
        self.getenv = kwargs.pop('getenv', True)
        self.getshell = kwargs.pop('getshell', True)
        self.auth = kwargs.pop('auth', None)

        if self.auth == None:
            self.auth = BaseClientAuth()

        self.storage_options = kwargs.pop('storage_options', {})

        if all(isinstance(a, (tuple, list)) for a in args):
            args = list(flatten(args))
        if len(args) == 1:
            args = args[0]

        self._state = create_state(name, args, ttl,
                                   auth=self.auth,
                                   getenv=self.getenv,
                                   getshell=self.getshell,
                                   storage_options=self.storage_options)
        self.metadata = {}
        self.reload()

    def reload(self):
        self.name, self._children, self._entries, self._plugins = self._state.refresh()
        self.metadata = self._state.metadata
        self._all_entries = {source: cat_entry for _, source, cat_entry
                             in self.walk(leaves=True) }
        self._entry_tree = make_prefix_tree(self._all_entries)

    @property
    def version(self):
        # default version for pre-v1 files
        return self.metadata.get('version', 1)

    @property
    def changed(self):
        return self._state.changed()

    @reload_on_change
    def walk(self, leaves=True):
        visited, queue = set(), [self]
        while queue:
            catalog = queue.pop(0)
            if catalog not in visited:
                visited.add(catalog)
                queue.extend(set(catalog._children.values()) - visited)
                if leaves:
                    for source in catalog._entries:
                        yield catalog, source, catalog._entries[source]
                else:
                    yield catalog

    def get_catalog(self, name):
        for catalog in self.walk(leaves=False):
            if catalog.name == name:
                return catalog
        raise KeyError(name)

    @reload_on_change
    def _get_entry(self, name):
        return self._all_entries[name]

    @reload_on_change
    def _get_entries(self):
        return self._all_entries

    @reload_on_change
    def _get_entry_tree(self):
        return self._entry_tree

    def __iter__(self):
        """Return an iterator over catalog entries."""
        return iter(self._get_entries())

    def __dir__(self):
        return list(self._get_entry_tree().keys())

    def __getattr__(self, item):
        subtree = self._get_entry_tree()[item]
        if isinstance(subtree, dict):
            return CatalogSubtree(subtree)
        else:
            return subtree  # is catalog entry

    def __getitem__(self, key):
        """Return a catalog entry by name.
        
        Can also use attribute syntax, like ``cat.entry_name``.
        """
        return getattr(self, key)

    @property
    @reload_on_change
    def plugins(self):
        return self._plugins


class CatalogSubtree(object):
    def __init__(self, subtree):
        self._subtree = subtree

    def __dir__(self):
        return list(self._subtree.keys())

    def __getattr__(self, item):
        subtree = self._subtree[item]
        if isinstance(subtree, dict):
            return CatalogSubtree(subtree)
        else:
            return subtree  # is catalog entry
