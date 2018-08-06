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
                              chunks=self.chunks,
                              datashape=None)

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
            self.arr = da.Array(dask, name, self.chunks, self.dtype, self.shape)

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
