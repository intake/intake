# Base classes for Data Loader interface
from .cache import make_caches

class Schema(dict):
    """Holds details of data description for any type of data-source

    This should always be pickleable, so that it can be sent from a server
    to a client, and contain all information needed to recreate a RemoteSource
    on the client.
    """

    def __init__(self, **kwargs):
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


class DataSource(object):
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
            c._cache_dir = cache_dir

    def __new__(cls, *args, **kwargs):
        """Capture creation args when instantiating"""
        o = object.__new__(cls)
        # automatically capture __init__ arguments for pickling
        o._captured_init_args = args
        o._captured_init_kwargs = kwargs

        # monkey for requests keywords
        for key in ['auth', 'verify']:
            if (kwargs and kwargs.get('storage_options', None)
                    and key in kwargs['storage_options']):

                if isinstance(kwargs['storage_options'][key], list):
                    kwargs['storage_options'][key] = tuple(
                        kwargs['storage_options'][key])
        return o

    def __getstate__(self):
        return dict(args=self._captured_init_args,
                    kwargs=self._captured_init_kwargs)

    def __setstate__(self, state):
        self.__init__(*state['args'], **state['kwargs'])

    def __init__(self, metadata=None):
        # default data
        self.metadata = metadata or {}
        if isinstance(self.metadata, dict):
            self.cache = make_caches(self.name, self.metadata.get('cache'))
        self.datashape = None
        self.dtype = None
        self.shape = None
        self.npartitions = 0
        self._schema = None

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
        try:
            from ruamel.yaml import dump
        except:
            from ruamel_yaml import dump
        import inspect
        kwargs = self._captured_init_kwargs.copy()
        meta = kwargs.pop('metadata', self.metadata) or {}
        # TODO: inspect.signature is py3 only
        kwargs.update(dict(zip(inspect.signature(self.__init__).parameters,
                      self._captured_init_args)))
        data = {'sources': {self.name: {
            'driver': self.__class__.name,
            'description': self.description or "",
            'metadata': meta,
            'args': kwargs
        }}}
        if with_plugin:
            data['plugins'] = {
                'source': [{'module': self.__module__}]}
        return dump(data, default_flow_style=False)

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
        """Return a (offset_tuple, container) corresponding to i-th partition.

        Offset tuple is of same length as shape.

        By default, assumes i should be an integer between zero and npartitions;
        override for more complex indexing schemes.
        """
        self._load_metadata()
        if i < 0 or i >= self.npartitions:
            raise IndexError('%d is out of range' % i)

        return self._get_partition(i)

    def to_dask(self):
        """Return a dask container for this data source"""
        pass

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
