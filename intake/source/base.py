#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
''' Base classes for Data Loader interface

'''

from .cache import make_caches
from ..utils import make_path_posix, DictSerialiseMixin
import sys
PY2 = sys.version_info[0] == 2


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
        kwargs: typically include datashape, dtype, shape
        """
        super(Schema, self).__init__(**kwargs)
        for field in ['datashape', 'dtype', 'extra_metadata', 'shape']:
            # maybe a default-dict
            if field not in self:
                self[field] = None

    def __repr__(self):
        return ("<Schema instance>\n"
                "dtype: {}\n"
                "shape: {}\n"
                "metadata: {}"
                "".format(self.dtype, self.shape, self.extra_metadata))

    def __getattr__(self, item):
        return self[item]


class DataSource(DictSerialiseMixin):
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
    datashape = None
    description = None

    @property
    def cache_dirs(self):
        return [c._cache_dir for c in self.cache]

    def set_cache_dir(self, cache_dir):
        for c in self.cache:
            c._cache_dir = make_path_posix(cache_dir)

    def __init__(self, metadata=None):
        # default data
        self.metadata = metadata or {}
        if isinstance(self.metadata, dict):
            storage_options = self._captured_init_kwargs.get('storage_options',
                                                             {})
            self.cache = make_caches(self.name, self.metadata.get('cache'),
                                     catdir=self.metadata.get('catalog_dir',
                                                              None),
                                     storage_options=storage_options)
        self.datashape = None
        self.dtype = None
        self.shape = None
        self.npartitions = 0
        self._schema = None
        self.catalog_object = None
        self.on_server = False
        self.cat = None  # the cat from which this source was made

    def _get_cache(self, urlpath):
        if len(self.cache) == 0:
            return [urlpath]
        return [c.load(urlpath) for c in self.cache]

    def _get_schema(self):
        """Subclasses should return an instance of base.Schema"""
        raise Exception('Subclass should implement _get_schema()')

    def _get_partition(self, i):
        """Subclasses should return a container object for this partition

        This function will never be called with an out-of-range value for i.
        """
        raise Exception('Subclass should implement _get_partition()')

    def _close(self):
        """Subclasses should close all open resources"""
        raise Exception('Subclass should implement _close()')

    # These methods are implemented from the above two methods and do not need
    # to be overridden unless custom behavior is required

    def _load_metadata(self):
        """load metadata only if needed"""
        if self._schema is None:
            self._schema = self._get_schema()
            self.datashape = self._schema.datashape
            self.dtype = self._schema.dtype
            self.shape = self._schema.shape
            self.npartitions = self._schema.npartitions
            self.metadata.update(self._schema.extra_metadata)

    def _yaml(self, with_plugin=False):
        import inspect
        kwargs = self._captured_init_kwargs.copy()
        meta = kwargs.pop('metadata', self.metadata) or {}
        if PY2:
            kwargs.update(dict(zip(inspect.getargspec(self.__init__).args,
                          self._captured_init_args)))
        else:
            kwargs.update(dict(zip(inspect.signature(self.__init__).parameters,
                                   self._captured_init_args)))
        data = {'sources': {self.name: {
            'driver': self.classname,
            'description': self.description or "",
            'metadata': meta,
            'args': kwargs
        }}}
        if with_plugin:
            data['plugins'] = {
                'source': [{'module': self.__module__}]}
        return data

    def yaml(self, with_plugin=False):
        """Return YAML representation of this data-source

        The output may be roughly appropriate for inclusion in a YAML
        catalog. This is a best-effort implementation

        Parameters
        ----------
        with_plugin: bool
            If True, create a "plugins" section, for cases where this source
            is created with a plugin not expected to be in the global Intake
            registry.
        """
        from yaml import dump
        data = self._yaml(with_plugin=with_plugin)
        return dump(data, default_flow_style=False)

    @property
    def plots(self):
        """List custom associated quick-plots """
        return list(self.metadata.get('plots', {}))

    def discover(self):
        """Open resource and populate the source attributes."""
        self._load_metadata()

        return dict(datashape=self.datashape,
                    dtype=self.dtype,
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

    def close(self):
        """Close open resources corresponding to this data source."""
        self._close()

    # Boilerplate to make this object also act like a context manager
    def __enter__(self):
        self._load_metadata()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

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

    def persist(self, ttl=None, **kwargs):
        """Save data from this source to local persistent storage"""
        from ..container import container_map
        from ..container.persist import PersistStore
        import time
        if 'original_tok' in self.metadata:
            raise ValueError('Cannot persist a source taken from the persist '
                             'store')
        method = container_map[self.container]._persist
        store = PersistStore()
        out = method(self, path=store.getdir(self), **kwargs)
        out.description = self.description
        metadata = {'timestamp': time.time(),
                    'original_metadata': self.metadata,
                    'original_source': self.__getstate__(),
                    'original_name': self.name,
                    'original_tok': self._tok,
                    'persist_kwargs': kwargs,
                    'ttl': ttl,
                    'cat': {} if self.cat is None else self.cat.__getstate__()}
        out.metadata = metadata
        out.name = self.name
        store.add(self._tok, out)
        return out

    def export(self, path, **kwargs):
        """Save this data for sharing with other people

        Creates a copy of the data in a format appropriate for its container,
        in the location specified (which can be remote, e.g., s3). Returns
        a YAML representation of this saved dataset, so that it can be put
        into a catalog file.
        """
        from ..container import container_map
        import time
        method = container_map[self.container]._persist
        # may need to create path - access file-system method
        out = method(self, path=path, **kwargs)
        out.description = self.description
        metadata = {'timestamp': time.time(),
                    'original_metadata': self.metadata,
                    'original_source': self.__getstate__(),
                    'original_name': self.name,
                    'original_tok': self._tok,
                    'persist_kwargs': kwargs}
        out.metadata = metadata
        out.name = self.name
        return out.yaml()

    def get_persisted(self):
        from ..container.persist import store
        return store[self._tok]()

    @staticmethod
    def _persist(source, path, **kwargs):
        """To be implemented by 'container' sources for locally persisting"""
        raise NotImplementedError

    @property
    def has_been_persisted(self):
        from ..container.persist import store
        return self._tok in store

    @property
    def is_persisted(self):
        from ..container.persist import store
        return self.metadata.get('original_tok', None) in store


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


class AliasSource(DataSource):
    """Refer to another named source, unmodified

    The purpose of an Alias is to be able to refer to other source(s) in the
    same catalog, perhaps leaving the choice of which target to load up to the
    user. This source makes no sense outside of a catalog.

    In this case, the output of the target source is not modified, but this
    class acts as a prototype 'derived' source for processing the output of
    some standard driver.

    After initial discovery, the source's container and other details will be
    updated from the target; initially, the AliasSource container is not
    any standard.
    """
    container = 'other'
    version = 1
    name = 'alias'

    def __init__(self, target, mapping=None, metadata=None, **kwargs):
        """

        Parameters
        ----------
        target: str
            Name of the source to load, must be a key in the same catalog
        mapping: dict or None
            If given, use this to map the string passed as ``target`` to
            entries in the catalog
        metadata: dict or None
            Extra metadata to associate
        kwargs: passed on to the target
        """
        super(AliasSource, self).__init__(metadata)
        self.target = target
        self.mapping = mapping or {target: target}
        self.kwargs = kwargs
        self.metadata = metadata
        self.source = None

    def _get_source(self):
        if self.catalog_object is None:
            raise ValueError('AliasSource cannot be used outside a catalog')
        if self.source is None:
            self.source = self.catalog_object[self.mapping[self.target]](
                metadata=self.metadata, **self.kwargs)
            self.metadata = self.source.metadata.copy()
            self.container = self.source.container
            self.partition_access = self.source.partition_access
            self.description = self.source.description
            self.datashape = self.source.datashape

    def discover(self):
        self._get_source()
        return self.source.discover()

    def read(self):
        self._get_source()
        return self.source.read()

    def read_partition(self, i):
        self._get_source()
        return self.source.read_partition(i)

    def read_chunked(self):
        self._get_source()
        return self.source.read_chunked()

    def to_dask(self):
        self._get_source()
        return self.source.to_dask()
