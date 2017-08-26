# Base classes for Data Loader interface

class Plugin:
    def open(self, *args, **kwargs):
        raise Exception('Implement open')


class DataSource:
    def __init__(self, datashape=None, dtype=None, shape=None, container=None, description=None):
        self.datashape = datashape
        self.dtype = dtype
        self.shape = shape
        self.container = container
        self.description = description

    def discover(self):
        raise Exception('Implement discover')

    def read(self):
        raise Exception('Implement read')

    def read_chunks(self):
        raise Exception('Implement read_chunks')

    def to_dask(self):
        raise Exception('Implement to_dask')

    def close(self):
        raise Exception('Implement close')

    # Boilerplate to make this object also act like a context manager
    def __enter__(self):
        return self # Nothing to do here

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
