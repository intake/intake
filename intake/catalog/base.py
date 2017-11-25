import os.path
import sys
import time

from requests.compat import urljoin, urlparse
import msgpack
import requests
import yaml

from .local import parse_catalog_entry, parse_source_plugins
from .remote import RemoteCatalogEntry
from .utils import clamp, flatten, reload_on_change


class State(object):
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
    def __init__(self, name, observable, ttl):
        super(DirectoryState, self).__init__(name, observable, ttl)
        self.catalogs = []

    def refresh(self):
        catalogs = []
        for f in os.listdir(self.observable):
            if f.endswith('.yml') or f.endswith('.yaml'):
                catalogs.append(Catalog(os.path.join(self.observable, f)))

        self.catalogs = catalogs
        children = {catalog.name: catalog for catalog in self.catalogs}

        return self.name, children, {}, []

    def changed(self):
        modified = self.update_modification_time(os.path.getmtime(self.observable))
        return any([modified] + [catalog.changed for catalog in self.catalogs])


class RemoteState(State):
    def __init__(self, name, observable, ttl):
        super(RemoteState, self).__init__(name, observable, ttl)
        self.base_url = observable + '/'
        self.info_url = urljoin(self.base_url, 'v1/info')
        self.source_url = urljoin(self.base_url, 'v1/source')

    def refresh(self):
        name = urlparse(self.observable).netloc.replace('.', '_').replace(':', '_')

        response = requests.get(self.info_url)
        if response.status_code != 200:
            raise Exception('%s: status code %d' % (response.url, response.status_code))
        info = msgpack.unpackb(response.content, encoding=sys.getdefaultencoding())

        entries = {s['name']: RemoteCatalogEntry(url=self.source_url, **s) for s in info['sources']}

        return name, {}, entries, []


class LocalState(State):
    def __init__(self, name, observable, ttl):
        super(LocalState, self).__init__(name, observable, ttl)

    def refresh(self):
        name = os.path.splitext(os.path.basename(self.observable))[0].replace('.', '_')

        catalog_dir = os.path.dirname(os.path.abspath(self.observable))
        with open(self.observable, 'r') as f:
            cfg = yaml.safe_load(f.read())

        plugins = []
        if 'plugins' in cfg and 'source' in cfg['plugins']:
            plugins = parse_source_plugins(cfg['plugins']['source'], catalog_dir)

        entries = {
            key: parse_catalog_entry(value,
                                     catalog_plugin_registry=plugins,
                                     catalog_dir=catalog_dir)
            for key, value in cfg['sources'].items()
        }

        return name, {}, entries, plugins

    def changed(self):
        return self.update_modification_time(os.path.getmtime(self.observable))


class CollectionState(State):
    def __init__(self, name, observable, ttl):
        super(CollectionState, self).__init__(name, observable, ttl)
        self.catalogs = [Catalog(uri) for uri in self.observable]

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
    elif os.path.isdir(observable):
        return DirectoryState(name, observable, ttl)
    elif observable.endswith('.yml') or observable.endswith('.yaml'):
        return LocalState(name, observable, ttl)

    raise TypeError


class Catalog(object):
    def __init__(self, *args, name=None, ttl=1):
        args = list(flatten(args))
        args = args[0] if len(args) == 1 else args

        self._state = create_state(name, args, ttl)
        self.reload()

    def reload(self):
        self.name, self._children, self._entries, self._plugins = self._state.refresh()

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

    def get_catalogs(self):
        catalogs, _, _ = zip(*self.walk())
        return list(set([catalog.name for catalog in catalogs if catalog.name]))

    def get_entries(self):
        _, names, _ = zip(*self.walk())
        return list(set(names))

    def get_catalog(self, name):
        for catalog in self.walk(leaves=False):
            if catalog.name == name:
                return catalog
        raise KeyError(name)

    @reload_on_change
    def get_entry(self, name):
        return self._entries[name]

    def __iter__(self):
        return iter(self.get_catalogs()) if self._children else iter(self.get_entries())

    def __dir__(self):
        return self.get_catalogs() if self._children else self.get_entries()

    def __getattr__(self, item):
        return self.get_catalog(item) if self._children else self.get_entry(item)

    def __getitem__(self, item):
        return self.get_catalog(item) if self._children else self.get_entry(item)

    @property
    @reload_on_change
    def plugins(self):
        return self._plugins
