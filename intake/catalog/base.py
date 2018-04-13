import logging
import os.path
import time

import msgpack
import requests
from requests.compat import urljoin, urlparse
import six

from .local import CatalogConfig
from .remote import RemoteCatalogEntry
from .utils import clamp, flatten, reload_on_change, make_prefix_tree
logger = logging.getLogger('intake')


class State(object):
    """Base class representing the state of a catalog source"""

    def __init__(self, name, observable, ttl):
        self.name = name
        self.observable = observable
        self.ttl = clamp(ttl)
        self._modification_time = 0
        self._last_updated = 0

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

    def __init__(self, name, observable, ttl):
        super(DirectoryState, self).__init__(name, observable, ttl)
        self.catalogs = []
        self._last_files = []

    def refresh(self):
        catalogs = []

        if os.path.isdir(self.observable):
            self._last_files = []
            for f in os.listdir(self.observable):
                if f.endswith('.yml') or f.endswith('.yaml'):
                    path = os.path.join(self.observable, f)
                    try:
                        catalogs.append(Catalog(path))
                        self._last_files.append(path)
                    except Exception as e:
                        logger.warning("%s: %s" % (str(e), f))

        self.catalogs = catalogs
        children = {catalog.name: catalog for catalog in self.catalogs}

        return self.name, children, {}, []

    def changed(self):
        if not os.path.isdir(self.observable):
            return False
        modified = self.update_modification_time(
            os.path.getmtime(self.observable))
        # Were any files removed?
        modified = modified or any(not os.path.exists(filename)
                                   for filename in self._last_files)
        if modified:
            self.refresh()
        return any([modified] + [catalog.changed for catalog in self.catalogs])


class RemoteState(State):
    """The state of a remote Intake server"""

    def __init__(self, name, observable, ttl):
        super(RemoteState, self).__init__(name, observable, ttl)
        self.base_url = observable + '/'
        self.info_url = urljoin(self.base_url, 'v1/info')
        self.source_url = urljoin(self.base_url, 'v1/source')

    def refresh(self):
        name = urlparse(self.observable).netloc.replace(
            '.', '_').replace(':', '_')

        response = requests.get(self.info_url)
        if response.status_code != 200:
            raise Exception('%s: status code %d' % (response.url,
                                                    response.status_code))
        info = msgpack.unpackb(response.content, encoding='utf-8')

        entries = {s['name']: RemoteCatalogEntry(url=self.source_url, **s)
                   for s in info['sources']}

        return name, {}, entries, []


class LocalState(State):
    """The state of a catalog file on the local filesystem"""

    def __init__(self, name, observable, ttl):
        super(LocalState, self).__init__(name, observable, ttl)

    def refresh(self):
        cfg = CatalogConfig(self.observable)
        return cfg.name, {}, cfg.entries, cfg.plugins

    def changed(self):
        return self.update_modification_time(os.path.getmtime(self.observable))


class CollectionState(State):
    """The state of a collection of other states"""

    def __init__(self, name, observable, ttl):
        super(CollectionState, self).__init__(name, observable, ttl)
        # This name is a workaround to deal with issue that will be
        # solved in another PR
        self.catalogs = [Catalog(uri, name='cat%d' % i)
                         for i, uri in enumerate(self.observable)]

    def refresh(self):
        for catalog in self.catalogs:
            catalog.reload()
        name = None
        children = {catalog.name: catalog for catalog in self.catalogs}
        return name, children, {}, []

    def changed(self):
        return any([catalog.changed for catalog in self.catalogs])


def create_state(name, observable, ttl):
    if isinstance(observable, list):
        return CollectionState(name, observable, ttl)
    elif observable.startswith('http://') or observable.startswith('https://'):
        return RemoteState(name, observable, ttl)
    elif observable.endswith('.yml') or observable.endswith('.yaml'):
        return LocalState(name, observable, ttl)
    elif isinstance(observable, six.string_types):
        return DirectoryState(name, observable, ttl)

    raise TypeError


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
        """
        name = kwargs.get('name', None)
        ttl = kwargs.get('ttl', 1)

        args = list(flatten(args))
        args = args[0] if len(args) == 1 else args

        self._state = create_state(name, args, ttl)
        self.reload()

    def reload(self):
        self.name, self._children, self._entries, self._plugins = self._state.refresh()
        self._all_entries = {source: cat_entry for _, source, cat_entry
                             in self.walk(leaves=True) }
        self._entry_tree = make_prefix_tree(self._all_entries)

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
