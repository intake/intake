import logging
import time

import msgpack
import requests
from requests.compat import urljoin, urlparse
from dask.bytes import open_files

from ..auth.base import BaseClientAuth
from .entry import CatalogEntry
from .remote import RemoteCatalogEntry
from .utils import clamp, flatten, reload_on_change
logger = logging.getLogger('intake')


class RemoteState():
    """The state of a remote Intake server"""

    def __init__(self, name, observable, ttl, auth, getenv=True, getshell=True,
                 http_args=None):
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
        for name, item in self._get_entries().items():
            if item.container == 'catalog' and depth > 1:
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

    def __getattr__(self, item):
        return self._get_entry(item)

    def __getitem__(self, key):
        """Return a catalog entry by name.
        
        Can also use attribute syntax, like ``cat.entry_name``.
        """
        if '.' in key:
            out = self
            for k in key.split('.'):
                if isinstance(out, CatalogEntry):
                    out = out()  # default parameters
                if not isinstance(out, Catalog):
                    raise ValueError("Attempt to recurse into non-catalog")
                out = getattr(out, k)
            return out
        else:
            return getattr(self, key)

    @property
    @reload_on_change
    def plugins(self):
        return self._plugins
