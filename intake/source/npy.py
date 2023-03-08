# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from .base import DataSource, Schema


class NPySource(DataSource):
    """Read numpy binary files into an array

    Prototype source showing example of working with arrays

    Each file becomes one or more partitions, but partitioning within a file
    is only along the largest dimension, to ensure contiguous data.
    """

    container = "ndarray"
    name = "numpy"
    version = "0.0.1"
    partition_access = True

    def __init__(self, path, dtype=None, shape=None, chunks=None, storage_options=None, metadata=None):
        """
        The parameters dtype and shape will be determined from the first
        file, if not given.

        Parameters
        ----------
        path: str of list of str
            Location of data file(s), possibly including glob and protocol
            information
        dtype: str dtype spec
            In known, the dtype (e.g., "int64" or "f4").
        shape: tuple of int
            If known, the length of each axis
        chunks: int
            Size of chunks within a file along biggest dimension - must exactly divide
            each file, or None for one partition per file.
        storage_options: dict
            Passed to file-system backend.
        """
        self.path = path
        self.shape = shape
        self.dtype = dtype
        self.storage = storage_options or {}
        self.files = None
        self._chunks = chunks
        self._arrs = None
        self._arr = None
        super(NPySource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from fsspec import open_files

        if self.files is None:
            path = self._get_cache(self.path)[0]

            self.files = open_files(path, "rb", compression=None, **self.storage)
            if self.dtype is None or self.shape is None:
                arr = NumpyAccess(self.files[0])
                self.shape = arr.shape
                self.dtype = arr.dtype
            self.chunks = self._chunks or self.shape[0]
        shape = list(self.shape)
        parts_per_file = (shape[0] // self.chunks) + (shape[0] % self.chunks > 0)
        if len(self.files) > 1:
            shape = [len(self.files)] + shape
            chunks = ((1,) * len(self.files),) + ((self.chunks,) * parts_per_file,) + tuple((s,) for s in self.shape[1:])
        else:
            shape = shape
            chunks = ((self.chunks,) * parts_per_file,) + tuple((s,) for s in self.shape[1:])

        print()
        return Schema(dtype=str(self.dtype), shape=tuple(shape), extra_metadata=self.metadata, npartitions=parts_per_file * len(self.files), chunks=chunks)

    def _get_partition(self, i):
        if isinstance(i, (list, tuple)):
            i = i[0]
        parts_per_file = (self.shape[0] // self.chunks) + (self.shape[0] % self.chunks > 0)
        nfile = i // parts_per_file
        arr = NumpyAccess(self.files[nfile])
        infile = i % parts_per_file
        return arr[self.chunks * infile : self.chunks * (infile + 1)]

    def read_partition(self, i):
        return self._get_partition(i)

    def to_dask(self):
        import dask.array as da

        self._get_schema()
        arrs = [NumpyAccess(f, self.shape, self.dtype) for f in self.files]

        if len(arrs) > 1:
            chunks = (self._chunks or -1,) + (-1,) * (len(self.shape) - 1)
        else:
            chunks = (self._chunks or -1,) + (-1,) * (len(self.shape) - 2)
        self._arrs = [da.from_array(arr, chunks) for arr in arrs]

        self._arr = da.stack(self._arrs)
        return self._arr

    def read(self):
        import numpy as np

        self._get_schema()
        with self.files as ff:
            arrs = [np.load(f) for f in ff]
        return np.stack(arrs)

    def _close(self):
        self._arrs = None
        self._arr = None


class NumpyAccess(object):
    def __init__(self, f, shape=None, dtype=None, order="C", offset=None):
        self.f = f
        self.shape = shape
        self.dtype = dtype
        self.order = order
        self.offset = offset
        if self.shape is None or dtype is None or offset is None:
            self._get_info()
        self.ndim = len(self.shape)

    def __getitem__(self, item):
        import copy

        import numpy as np

        if isinstance(item, tuple):
            item = item[0]
        first = (item.stop or self.shape[0]) - (item.start or 0)
        block = item.start or 0
        count = first
        for i in self.shape[1:]:
            block *= i
            count *= i
        if count == 0:
            return np.array([], dtype=self.dtype).reshape(*(-1,) + self.shape[1:])

        start = self.offset + block * self.dtype.itemsize
        shape = (first,) + self.shape[1:]
        fn = copy.copy(self.f)  # makes local copy to avoid close while reading
        with fn as f:
            f.seek(start)
            data = f.read(count * self.dtype.itemsize)
            return np.frombuffer(data, dtype=self.dtype).reshape(shape)

    def _get_info(self):
        from numpy.lib import format

        with self.f as fp:
            version = format.read_magic(fp)
            format._check_version(version)

            shape, fortran_order, dtype = format._read_array_header(fp, version)
            self.shape = shape
            self.dtype = dtype
            self.order = "F" if fortran_order else "C"
            self.offset = fp.tell()
