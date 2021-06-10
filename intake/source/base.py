#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
"""
Base classes for Data Loader interface
"""

from yaml import dump

from .cache import make_caches
from ..utils import make_path_posix, DictSerialiseMixin, pretty_describe


class Schema(dict):
    """Holds details of data description for any type of data-source

    This should always be pickleable, so that it can be sent from a server
    to a client, and contain all information needed to recreate a RemoteSource
    on the client.
    """

    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        kwargs: typically include dtype, shape
        """
        super(Schema, self).__init__(**kwargs)
        for field in ['dtype', 'shape']:
            # maybe a default-dict
            if field not in self:
                self[field] = None
        if 'npartitions' not in self:
            self['npartitions'] = 1
        if 'extra_metadata' not in self:
            self['extra_metadata'] = {}

    def __repr__(self):
        return ("<Schema instance>\n"
                "dtype: {}\n"
                "shape: {}\n"
                "metadata: {}"
                "".format(self.dtype, self.shape, self.extra_metadata))

    def __getattr__(self, item):
        return self[item]


class NoEntry(AttributeError):
    pass


class CacheMixin:
    """Allows "old style" caching for Data Source"""
    _cache = None

    @property
    def cache_dirs(self):
        return [c._cache_dir for c in self.cache]

    def set_cache_dir(self, cache_dir):
        for c in self.cache:
            c._cache_dir = make_path_posix(cache_dir)

    @property
    def cache(self):
        if self._cache is None:
            self._cache = make_caches(self.name, self.metadata.get('cache'),
                                      catdir=self.metadata.get('catalog_dir',
                                                             None),
                                      storage_options=self.storage_options)
        return self._cache

    @cache.setter
    def cache(self, csh):
        self._cache = csh

    def _get_cache(self, urlpath):
        if len(self.cache) == 0:
            return [urlpath]
        return [c.load(urlpath) for c in self.cache]


class HoloviewsMixin:
    """Adds plotting and GUI to DataSource"""

    @property
    def plots(self):
        """List custom associated quick-plots """
        return list(self.metadata.get('plots', {}))

    @property
    def gui(self):
        """Source GUI, with parameter selection and plotting"""
        return self.entry.gui

    @property
    def plot(self):
        """
        Returns a hvPlot object to provide a high-level plotting API.

        To display in a notebook, be sure to run ``intake.output_notebook()``
        first.
        """
        try:
            from hvplot import hvPlot
        except ImportError:
            raise ImportError("The intake plotting API requires hvplot."
                              "hvplot may be installed with:\n\n"
                              "`conda install -c pyviz hvplot` or "
                              "`pip install hvplot`.")
        metadata = self.metadata.get('plot', {})
        fields = self.metadata.get('fields', {})
        for attrs in fields.values():
            if 'range' in attrs:
                attrs['range'] = tuple(attrs['range'])
        metadata['fields'] = fields
        plots = self.metadata.get('plots', {})
        return hvPlot(self, custom_plots=plots, **metadata)

    @property
    def hvplot(self):
        """
        Returns a hvPlot object to provide a high-level plotting API.
        """
        return self.plot


class PersistMixin:
    """Adds interaction with PersistStore to DataSource"""

    def get_persisted(self):
        from ..container.persist import store
        from dask.base import tokenize
        return store[tokenize(self)]()

    @staticmethod
    def _persist(source, path, **kwargs):
        """To be implemented by 'container' sources for locally persisting"""
        raise NotImplementedError

    @property
    def has_been_persisted(self):
        from ..container.persist import store
        from dask.base import tokenize
        return tokenize(self) in store

    @property
    def is_persisted(self):
        from ..container.persist import store
        return self.metadata.get('original_tok', None) in store

    def persist(self, ttl=None, **kwargs):
        """Save data from this source to local persistent storage

        Parameters
        ----------
        ttl: numeric, optional
            Time to live in seconds. If provided, the original source will
            be accessed and a new persisted version written transparently
            when more than ``ttl`` seconds have passed since the old persisted
            version was written.
        kargs: passed to the _persist method on the base container.
        """
        from ..container.persist import PersistStore
        from dask.base import tokenize
        if 'original_tok' in self.metadata:
            raise ValueError('Cannot persist a source taken from the persist '
                             'store')
        if ttl is not None and not isinstance(ttl, (int, float)):
            raise ValueError('Cannot persist using a time to live that is '
                             f'non-numeric. User-provided ttl was {ttl}')
        store = PersistStore()
        out = self._export(store.getdir(self), **kwargs)
        out.metadata.update({
            'ttl': ttl,
            'cat': {} if self.cat is None else self.cat.__getstate__()
        })
        out.name = self.name
        store.add(tokenize(self), out)
        return out


class DataSourceBase(DictSerialiseMixin):
    """An object which can produce data

    This is the base class for all Intake plugins, including catalogs and
    remote (server) data objects. To produce a new plugin commonly involves
    subclassing this definition and overriding some or all of the methods.

    This class is not useful in itself, most methods raise NotImplemented.
    """

    name = None
    version = None
    container = None
    partition_access = False
    description = None
    dtype = None
    shape = None
    npartitions = 0
    _schema = None
    on_server = False
    cat = None  # the cat from which this source was made
    _entry = None

    def __init__(self, storage_options=None, metadata=None):
        # default data
        self.metadata = metadata or {}
        if isinstance(self.metadata, dict) and storage_options is None:
            storage_options = self._captured_init_kwargs.get('storage_options',
                                                             {})
        self.storage_options = storage_options

    def _get_schema(self):
        """Subclasses should return an instance of base.Schema"""
        raise NotImplementedError

    def _get_partition(self, i):
        """Subclasses should return a container object for this partition

        This function will never be called with an out-of-range value for i.
        """
        raise NotImplementedError

    def _close(self):
        """Subclasses should close all open resources"""
        raise NotImplementedError

    def _load_metadata(self):
        """load metadata only if needed"""
        if self._schema is None:
            self._schema = self._get_schema()
            self.dtype = self._schema.dtype
            self.shape = self._schema.shape
            self.npartitions = self._schema.npartitions
            self.metadata.update(self._schema.extra_metadata)

    def _yaml(self):
        import inspect
        kwargs = self._captured_init_kwargs.copy()
        meta = kwargs.pop('metadata', self.metadata) or {}
        kwargs.update(dict(zip(inspect.signature(self.__init__).parameters,
                           self._captured_init_args)))
        data = {
            'sources':
                {self.name: {
                   'driver': self.classname,
                   'description': self.description or "",
                   'metadata': meta,
                   'args': kwargs
                }}}
        return data

    def yaml(self):
        """Return YAML representation of this data-source

        The output may be roughly appropriate for inclusion in a YAML
        catalog. This is a best-effort implementation
        """
        data = self._yaml()
        return dump(data, default_flow_style=False)

    def _ipython_display_(self):
        """Display the entry as a rich object in an IPython session."""
        from IPython.display import display
        data = self._yaml()['sources']
        contents = dump(data, default_flow_style=False)
        display({
            'application/yaml': contents,
            'text/plain': pretty_describe(contents)
        }, metadata={
            'application/json': {'root': self.name}
        }, raw=True)

    def __repr__(self):
        return self.yaml()

    @property
    def is_persisted(self):
        """The base class does not interact with persistence"""
        return False

    @property
    def has_been_persisted(self):
        """The base class does not interact with persistence"""
        return False

    def _get_cache(self, urlpath):
        """The base class does not interact with caches"""
        return urlpath

    def discover(self):
        """Open resource and populate the source attributes."""
        self._load_metadata()

        return dict(dtype=self.dtype,
                    shape=self.shape,
                    npartitions=self.npartitions,
                    metadata=self.metadata)

    def read(self):
        """Load entire dataset into a container and return it"""
        if not self.partition_access or self.npartitions == 1:
            return self._get_partition(0)
        else:
            raise NotImplementedError

    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        self._load_metadata()
        for i in range(self.npartitions):
            yield self._get_partition(i)

    def read_partition(self, i):
        """Return a part of the data corresponding to i-th partition.

        By default, assumes i should be an integer between zero and npartitions;
        override for more complex indexing schemes.
        """
        self._load_metadata()
        if i < 0 or i >= self.npartitions:
            raise IndexError('%d is out of range' % i)

        return self._get_partition(i)

    def to_dask(self):
        """Return a dask container for this data source"""
        raise NotImplementedError

    def to_spark(self):
        """Provide an equivalent data object in Apache Spark

        The mapping of python-oriented data containers to Spark ones will be
        imperfect, and only a small number of drivers are expected to be able
        to produce Spark objects. The standard arguments may b translated,
        unsupported or ignored, depending on the specific driver.

        This method requires the package intake-spark
        """
        raise NotImplementedError

    @property
    def entry(self):
        if self._entry is None:
            raise NoEntry("Source was not made from a catalog entry")
        return self._entry

    def configure_new(self, **kwargs):
        """Create a new instance of this source with altered arguments

        Enables the picking of options and re-evaluating templates from any
        user-parameters associated with
        this source, or overriding any of the init arguments.

        Returns a new data source instance. The instance will be recreated from
        the original entry definition in a catalog **if** this source was originally
        created from a catalog.
        """
        if self._entry is not None:
            kw = {k: v for k, v in self._captured_init_kwargs.items()
                  if k in self._passed_kwargs}
            kw.update(kwargs)
            obj = self._entry(**kw)
            obj._entry = self._entry
            return obj
        else:
            kw = self._captured_init_kwargs.copy()
            kw.update(kwargs)
            return type(self)(*self._captured_init_args, **kw)

    __call__ = get = configure_new  # compatibility aliases

    def describe(self):
        """Description from the entry spec"""
        return self.entry.describe()

    def close(self):
        """Close open resources corresponding to this data source."""
        self._close()

    # Boilerplate to make this object also act like a context manager
    def __enter__(self):
        self._load_metadata()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def export(self, path, **kwargs):
        """Save this data for sharing with other people

        Creates a copy of the data in a format appropriate for its container,
        in the location specified (which can be remote, e.g., s3).

        Returns the resultant source object, so that you can, for instance,
        add it to a catalog (``catalog.add(source)``) or get its YAML
        representation (``.yaml()``).
        """
        return self._export(path, **kwargs)

    def _export(self, path, **kwargs):
        from ..container import container_map
        import time
        from dask.base import tokenize
        method = container_map[self.container]._persist
        # may need to create path - access file-system method
        out = method(self, path=path, **kwargs)
        out.description = self.description
        metadata = {'timestamp': time.time(),
                    'original_metadata': self.metadata,
                    'original_source': self.__getstate__(),
                    'original_name': self.name,
                    'original_tok': tokenize(self),
                    'persist_kwargs': kwargs}
        out.metadata = metadata
        out.name = self.name
        return out


class DataSource(CacheMixin, HoloviewsMixin, PersistMixin, DataSourceBase):
    """A Data Source will all optional functionality

    When subclassed, child classes will have the base data source functionality,
    plus caching, plotting and persistence abilities.
    """
    pass


class PatternMixin(object):
    """Helper class to provide file-name parsing abilities to a driver class"""
    @property
    def path_as_pattern(self):
        if hasattr(self, '_path_as_pattern'):
            return self._path_as_pattern
        raise KeyError('Plugin needs to set `path_as_pattern`'
                       ' before setting urlpath')

    @path_as_pattern.setter
    def path_as_pattern(self, path_as_pattern):
        self._path_as_pattern = path_as_pattern

    @property
    def urlpath(self):
        return self._urlpath

    @urlpath.setter
    def urlpath(self, urlpath):
        from .utils import path_to_glob

        if hasattr(self, '_original_urlpath'):
            self._urlpath = urlpath
            return

        self._original_urlpath = urlpath

        if self.path_as_pattern:
            self._urlpath = path_to_glob(urlpath)
        else:
            self._urlpath = urlpath

        if isinstance(self.path_as_pattern, bool):
            if isinstance(urlpath, str) and self._urlpath == urlpath:
                self.path_as_pattern = False

    @property
    def pattern(self):
        from .utils import path_to_pattern

        if isinstance(self.path_as_pattern, str):
            return self.path_as_pattern
        elif self.path_as_pattern:
            return path_to_pattern(self._original_urlpath, self.metadata)
        return
