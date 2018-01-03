# Base classes for Data Loader interface
from collections import namedtuple

import pandas as pd
import numpy as np
import dask
import dask.bag


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


Schema = namedtuple('Schema', ['datashape', 'dtype', 'shape', 'npartitions', 'extra_metadata'])


class DataSource(object):

    def __new__(cls, *args, **kwargs):
        o = object.__new__(cls)
        # automatically capture __init__ arguments for pickling
        o._captured_init_args = args
        o._captured_init_kwargs = kwargs

        return o

    def __getstate__(self):
        return dict(args=self._captured_init_args, kwargs=self._captured_init_kwargs)

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
        '''Subclasses should extend this method to return an instance of base.Schema'''
        raise Exception('Subclass should implement _get_schema()')

    def _get_partition(self, i):
        '''Subclasses should extend this method to return a container object for this partition'''
        raise Exception('Subclass should implement _get_partition()')
        
    def _close(self):
        '''Subclasses should extend this method to close all open resources'''
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
        '''Open resource and populate the datashape, dtype, shape, npartitions attributes.'''
        self._load_metadata()

        return dict(datashape=self.datashape, 
                    dtype=self.dtype, 
                    shape=self.shape, 
                    npartitions=self.npartitions,
                    metadata=self.metadata)

    def read(self):
        '''Load entire dataset into a container and return it'''
        self._load_metadata()

        parts = [self._get_partition(i) for i in range(self.npartitions)]

        return self._merge(parts)

    def _merge(self, parts):
        if self.container == 'dataframe':
            return pd.concat(parts, ignore_index=True)
        elif self.container == 'python':
            # This seems to be the fastest way to do this for large lists
            all = []
            for p in parts:
                all.extend(p)
            return all
        elif self.container == 'ndarray':
            raise Exception('Need to implement ndarray case')

    def read_chunked(self):
        '''Return iterator over container fragments of data source'''
        self._load_metadata()
        for i in range(self.npartitions):
            yield self._get_partition(i) 

    def read_partition(self, i):
        '''Return a (offset_tuple, container) corresponding to i-th partition.

        Offset tuple is of same length as shape.
        '''
        self._load_metadata()
        return self._get_partition(i)

    def to_dask(self):
        '''Return a dask container for this data source'''
        self._load_metadata()

        delayed_get_partition = dask.delayed(self._get_partition)
        parts = [delayed_get_partition(i) for i in range(self.npartitions)]
        
        if self.container == 'dataframe':
            # Construct metadata
            meta = { name: arg[0] for name, arg in self.dtype.fields.items() }
            ddf = dask.dataframe.from_delayed(parts, meta=meta)

            return ddf
        elif self.container == 'python':
            return dask.bag.from_delayed(parts)
        else:
            raise Exception('Not implemented')

    def close(self):
        '''Close open resources corresponding to this data source.'''
        self._close()

    # Boilerplate to make this object also act like a context manager
    def __enter__(self):
        self._load_metadata()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
