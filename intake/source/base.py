# Base classes for Data Loader interface


class Plugin(object):
    def __init__(self, name, version, container, partition_access):
        self.name = name
        self.version = version
        self.container = container
        self.partition_access = partition_access

    def open(self, *args, **kwargs):
        raise Exception('Implement open')

    def separate_base_kwargs(self, kwargs):
        kwargs = kwargs.copy()

        base_keys = ['metadata']
        base_kwargs = {k: kwargs.pop(k, None) for k in base_keys}

        return base_kwargs, kwargs


class DataSource(object):
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

    def discover(self):
        '''Open resource and populate the datashape, dtype, shape, npartitions attributes.'''
        raise Exception('Implement discover')

    def read(self):
        '''Load entire dataset into a container and return it'''
        raise Exception('Implement read')

    def read_chunked(self):
        '''Return iterator over container fragments of data source'''
        raise Exception('Implement read_chunked')

    def read_partition(self, i):
        '''Return a (offset_tuple, container) corresponding to i-th partition.

        Offset tuple is of same length as shape.
        '''
        raise Exception('Implement read_partition')

    def to_dask(self):
        '''Return a dask container for this data source'''
        raise Exception('Implement to_dask')

    def close(self):
        '''Close open resources corresponding to this data source.'''
        raise Exception('Implement close')

    # Boilerplate to make this object also act like a context manager
    def __enter__(self):
        return self  # Nothing to do here

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
