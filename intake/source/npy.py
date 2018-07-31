from .base import DataSource, Schema


class NPySource(DataSource):
    container = 'ndarray'
    name = 'numpy'
    version = '0.0.1'
    partition_access = True

    def __init__(self, path, shape=None, chunks=None, metadata=None):
        self.path = path
        self.shape = shape
        self._chunks = chunks if chunks is not None else -1
        self.chunks = None
        self.dtype = None
        self._arrs = None
        self._arr = None
        super(NPySource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from dask.bytes import open_files
        import dask.array as da
        if self._arr is None:
            files = open_files(self.path, 'rb', compression=None)
            if self.shape is None:
                arr = NumpyAccess(files[0])
                self.shape = arr.shape
                self.dtype = arr.dtype
                arrs = [arr] + [NumpyAccess(f, self.shape, self.dtype)
                                for f in files[1:]]
            else:
                arrs = [NumpyAccess(f, self.shape, self.dtype)
                        for f in files]
            self.chunks = (self._chunks, ) + (-1, ) * (len(self.shape) - 1)
            self._arrs = [da.from_array(arr, self.chunks) for arr in arrs]

            if len(self._arrs) > 1:
                self._arr = da.stack(self._arrs)
            else:
                self._arr = self._arrs[0]
            self.chunks = self._arr.chunks
        return Schema(dtype=str(self.dtype), shape=self.shape,
                      extra_metadata=self.metadata,
                      npartitions=self._arr.npartitions,
                      chunks=self.chunks)

    def _get_partition(self, i):
        if isinstance(i, list):
            i = tuple(i)
        return self._arr.blocks[i].compute()

    def read_partition(self, i):
        self._get_schema()
        return self._get_partition(i)

    def to_dask(self):
        self._get_schema()
        return self._arr

    def read(self):
        self._get_schema()
        return self._arr.compute()

    def _close(self):
        self._arrs = None
        self._arr = None


class NumpyAccess(object):

    def __init__(self, f, shape=None, dtype=None, order='C', offset=None):
        self.f = f
        self.shape = shape
        self.dtype = dtype
        self.order = order
        self.offset = None
        if self.shape is None or dtype is None or offset is None:
            self._get_info()

    def __getitem__(self, item):
        import numpy as np
        import copy
        item = item[0]
        first = item.stop - item.start
        block = item.start
        count = first
        for i in self.shape[1:]:
            block *= i
            count *= i

        start = self.offset + block * self.dtype.itemsize
        shape = (first, ) + self.shape[1:]
        fn = copy.copy(self.f)  # makes local copy to avoid close while reading
        with fn as f:
            f.seek(start)
            return np.fromfile(f, dtype=self.dtype, count=count).reshape(shape)

    def _get_info(self):
        from numpy.lib import format
        with self.f as fp:
            version = format.read_magic(fp)
            format._check_version(version)

            shape, fortran_order, dtype = format._read_array_header(fp, version)
            self.shape = shape
            self.dtype = dtype
            self.order = 'F' if fortran_order else 'C'
            self.offset = fp.tell()
