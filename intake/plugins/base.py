# Base classes for Data Loader interface

class Plugin:
	def open(self, *args, **kwargs):
		raise Exception('Implement open')


class DataSource:
	def __init__(self, datashape, dtype, shape, container, get_chunks_supported=False):
		self.datashape = datashape
		self.dtype = dtype
		self.shape = shape
		self.container = container
		self.get_chunks_supported = get_chunks_supported

	def read(self):
		raise Exception('Implement read')

	def read_chunks(self, chunksize):
		raise Exception('Implement read_chunks')

	def get_chunks(self, chunksize):
		raise Exception('Implement get_chunks')

	def close(self):
		raise Exception('Implement close')

	# Boilerplate to make this object also act like a context manager
	def __enter__(self):
		return self # Nothing to do here

	def __exit__(self, exc_type, exc_value, traceback):
		self.close()
