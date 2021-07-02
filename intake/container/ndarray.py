#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import datetime
import itertools
from .base import RemoteSource, Schema, get_partition


class RemoteArray(RemoteSource):
    """nd-array on an Intake server"""
    name = 'remote_ndarray'
    container = 'ndarray'

    def __init__(self, url, headers, **kwargs):
        super(RemoteArray, self).__init__(url, headers, **kwargs)
        self.npartitions = kwargs['npartitions']
        self.shape = tuple(kwargs['shape'])
        self.metadata = kwargs['metadata']
        self.dtype = kwargs['dtype']
        self.chunks = tuple(tuple(c)
                            for c in tuple(kwargs.get('chunks', (-1, ))))
        self.arr = None
        self._schema = Schema(npartitions=self.npartitions,
                              extra_metadata=self.metadata,
                              dtype=self.dtype,
                              shape=self.shape,
                              chunks=self.chunks)

    def _load_metadata(self):
        import dask.array as da
        if self.arr is None:
            name = 'remote-array-' + self._source_id
            nparts = (range(len(n)) for n in self.chunks)
            dask = {
                (name, ) + part: (get_partition, self.url, self.headers,
                                  self._source_id, self.container, part)

                for part in itertools.product(*nparts)
            }
            self.arr = da.Array(dask, name=name, chunks=self.chunks,
                                dtype=self.dtype, shape=self.shape)

        return self._schema

    def _get_partition(self, i):
        self._load_metadata()
        return self.arr.blocks[i].compute()

    def read_partition(self, i):
        self._load_metadata()
        return self._get_partition(i)

    def read(self):
        self._load_metadata()
        return self.arr.compute()

    def to_dask(self):
        self._load_metadata()
        return self.arr

    def _close(self):
        self.arr = None

    @staticmethod
    def _persist(source, path, component=None, storage_options=None,
                 **kwargs):
        """Save array to local persistent store

        Makes a parquet dataset out of the data using zarr.
        This then becomes a data entry in the persisted datasets catalog.
        Only works locally for the moment.

        Parameters
        ----------
        source: a DataSource instance to save
        name: str or None
            Key to refer to this persisted dataset by. If not given, will
            attempt to get from the source's name
        kwargs: passed on to zarr array creation, see
        """
        try:
            arr = source.to_dask()
        except NotImplementedError:
            arr = source.read()
        return RemoteArray._data_to_source(arr, path, component=None,
                                           storage_options=None, **kwargs)

    @staticmethod
    def _data_to_source(arr, path, component=None,  storage_options=None,
                        **kwargs):
        from dask.utils import is_arraylike
        from dask.array import to_zarr, from_array
        from ..source.zarr import ZarrArraySource
        if not is_arraylike(arr):
            raise NotImplementedError
        if not hasattr(arr, 'npartitions'):
            arr = from_array(arr, chunks='auto')
        to_zarr(arr, path, component=None,
                storage_options=storage_options, **kwargs)
        source = ZarrArraySource(path, storage_options, component)
        return source
