import h5py
import pandas as pd

from . import base

class Plugin(base.Plugin):
    def __init__(self):
        pass

    def open(self, filename, dataset):
        return HDF5Source(filename, dataset)


class HDF5Source(base.DataSource):
    def __init__(self, filename, dataset, start=None, end=None):
        self._init_args = dict(filename=filename, dataset=dataset, start=start, end=end)

        self._h5py_file = h5py.File(filename, 'r')
        self._h5py_dataset = self._h5py_file[dataset]


        dtype = self._h5py_dataset.dtype
        shape = self._h5py_dataset.shape
        datashape = None

        self._start = start if start is not None else 0
        self._end = end if end is not None else self._h5py_dataset.shape[0]
        shape = (self._end - self._start,) + self._h5py_dataset.shape[1:]


        # what kind of data should we return?
        if dtype.names is None or len(shape) > 1:
            container = 'ndarray'
        else:
            container = 'dataframe'
        super().__init__(datashape=datashape, 
                         dtype=dtype,
                         shape=shape,
                         container=container,
                         get_chunks_supported=True)

    def read(self):
        data = self._h5py_dataset[self._start:self._end]

        if self.container == 'dataframe':
            return pd.DataFrame(data)
        else:
            return data

    def _get_chunk_bounds(self, chunksize):
        for start in range(self._start, self._end, chunksize):
            end = min(start + chunksize, self._end)
            yield start, end

    def read_chunks(self, chunksize):
        for start, end in self._get_chunk_bounds(chunksize):
            chunk = self._h5py_dataset[start:end]
            if self.container == 'dataframe':
                yield pd.DataFrame(chunk)
            else:
                yield chunk

    def get_chunks(self, chunksize):
        data_sources = []
        for start, end in self._get_chunk_bounds(chunksize):
            args = self._init_args.copy()
            args['start'] = start
            args['end'] = end
            data_sources.append(HDF5Source(**args))

        return data_sources

    def close(self):
        self._h5py_file.close()

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
