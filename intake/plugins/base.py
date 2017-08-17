# Base classes for Data Loader interface

class Plugin:
	def open(self, *args, **kwargs):
		raise Exception('Implement open')


class DataSource:
	def __init__(self, datashape, dtype, shape, container):
		self.datashape = datashape
		self.dtype = dtype
		self.shape = shape
		self.container = container

	def read(self):
		pass

	def read_chunks(self, chunksize):
		pass

	def close(self):
		raise Exception('Implement close')

	# Boilerplate to make this object also act like a context manager
	def __enter__(self):
		return self # Nothing to do here

	def __exit__(self, exc_type, exc_value, traceback):
		self.close()
