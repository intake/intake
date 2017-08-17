import h5py
import pandas as pd

from . import base

class Plugin(base.Plugin):
	def __init__(self):
		pass

	def open(self, filename):
		h5py_file = h5py.File(filename, 'r')
		return HDF5Source(h5py_file)


class HDF5Source(base.DataSource):
	def __init__(self, h5py_file):
		self._h5py_file = h5py_file

	def getref(self, dataset):
		d = self._h5py_file[dataset]
		return HDF5DataRef(d)

	def close(self):
		self._h5py_file.close()


class HDF5DataRef(base.DataRef):
	def __init__(self, h5py_dataset):
		self._h5py_dataset = h5py_dataset

		dtype = h5py_dataset.dtype
		shape = h5py_dataset.shape
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
