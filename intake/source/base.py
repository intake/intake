# Base classes for Data Loader interface
import dask

from ..container import get_container_klass


class Plugin(object):
    """Data loader plugin

    Attributes
    ----------
    name : str
        Name of plugin.
    version : str
        Version number of plugin
    container : str
        Kind of container produced by this plugin: dataframe, ndarray, python
    partition_access : bool
        True if this plugin can split data into multiple partitions.
    """
    def __init__(self, name, version, container, partition_access):
        self.name = name
        self.version = version
        self.container = container
        self.partition_access = partition_access

    def open(self, *args, **kwargs):
        """Return a data source.
        
        Arguments are plugin specific.
        """
        raise NotImplementedError('Implement open')

    @staticmethod
    def separate_base_kwargs(kwargs):
        kwargs = kwargs.copy()

        base_keys = ['metadata']
        base_kwargs = {k: kwargs.pop(k, None) for k in base_keys}

        return base_kwargs, kwargs


class Schema(object):
    """Holds details of data description for any type of data-source"""
    def __init__(self, datashape=None, dtype=None, shape=None, npartitions=None,
                 extra_metadata=None):
        self.datashape = datashape
        self.dtype = dtype
        self.shape = shape
        self.npartitions = npartitions
        self.extra_metadata = extra_metadata

    def __repr__(self):
        return ("<Schema instance>\n"
                "dtype: {}\n"
                "shape: {}\n"
                "metadata: {}"
                "".format(self.dtype, self.shape, self.extra_metadata))


class DataSource(object):

    def __new__(cls, *args, **kwargs):
        o = object.__new__(cls)
        # automatically capture __init__ arguments for pickling
        o._captured_init_args = args
        o._captured_init_kwargs = kwargs

        return o

    def __getstate__(self):
        return dict(args=self._captured_init_args,
                    kwargs=self._captured_init_kwargs)

    def __setstate__(self, state):
        self.__init__(*state['args'], **state['kwargs'])

    def __init__(self, container, description=None, metadata=None):
        self.container = container
        self.description = description
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata

        self.datashape = None
        self.dtype = None
        self.shape = None
        self.npartitions = 0

        self._schema = None

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
        # load metadata only if needed
        if self._schema is None:
            self._schema = self._get_schema()
            self.datashape = self._schema.datashape
            self.dtype = self._schema.dtype
            self.shape = self._schema.shape
            self.npartitions = self._schema.npartitions
            self.metadata.update(self._schema.extra_metadata)

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
        self._load_metadata()

        parts = [self._get_partition(i) for i in range(self.npartitions)]

        return self._merge(parts)

    def _merge(self, parts):
        return get_container_klass(self.container).merge(parts)

    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        self._load_metadata()
        for i in range(self.npartitions):
            yield self._get_partition(i)

    def read_partition(self, i):
        """Return a (offset_tuple, container) corresponding to i-th partition.

        Offset tuple is of same length as shape.
        """
        self._load_metadata()
        if i < 0 or i >= self.npartitions:
            raise IndexError('%d is out of range' % i)

        return self._get_partition(i)

    def to_dask(self):
        """Return a dask container for this data source"""
        self._load_metadata()

        delayed_get_partition = dask.delayed(self._get_partition)
        parts = [delayed_get_partition(i) for i in range(self.npartitions)]

        return get_container_klass(self.container).to_dask(parts, self.dtype)

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
        Returns a HoloPlot object to provide a high-level plotting API.
        """
        try:
            from holoplot import HoloPlot
        except ImportError:
            raise ImportError("The intake plotting API requires holoplot."
                              "holoplot may be installed with:\n\n"
                              "`conda install -c pyviz holoplot` or "
                              "`pip install holoplot`.")
        metadata = self.metadata.get('plot', {})
        fields = self.metadata.get('fields', {})
        for attrs in fields.values():
            if 'range' in attrs:
                attrs['range'] = tuple(attrs['range'])
        metadata['fields'] = fields
        plots = self.metadata.get('plots', {})
        return HoloPlot(self, custom_plots=plots, **metadata)
