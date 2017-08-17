import h5py
import pandas as pd

from . import base

class Plugin(base.Plugin):
    def __init__(self):
        pass

    def open(self, filename, dataset):
        return HDF5Source(filename, dataset)


class HDF5Source(base.DataSource):
    def __init__(self, filename, dataset):
        self._h5py_file = h5py.File(filename, 'r')
        self._h5py_dataset = self._h5py_file[dataset]

        dtype = self._h5py_dataset.dtype
        shape = self._h5py_dataset.shape
        datashape = None

        # what kind of data should we return?
        if dtype.names is None or len(shape) > 1:
            container = 'ndarray'
        else:
            container = 'dataframe'
        super().__init__(datashape=datashape, 
                         dtype=dtype,
                         shape=shape,
                         container=container)

    def read(self):
        data = self._h5py_dataset[:]

        if self.container == 'dataframe':
            return pd.DataFrame(data)
        else:
            return data

    def read_chunks(self, chunksize):
        len_axis0 = self.shape[0]

        for start in range(0, len_axis0, chunksize):
            end = min(start + chunksize, len_axis0)

            chunk = self._h5py_dataset[start:end]
            if self.container == 'dataframe':
                yield pd.DataFrame(chunk)
            else:
                yield chunk

    def close(self):
        self._h5py_file.close()
