import pandas as pd

from . import base

class Plugin(base.Plugin):
    def __init__(self):
        pass

    def open(self, host_info, query):
        return PostgresSource(host_info, query)


class PostgresSource(base.DataSource):
    def __init__(self, host_info, query):
        self._init_args = dict(host_info=host_info, query=query)

        datashape = None
        dtype = None
        shape = None
        container = 'dataframe'
        super().__init__(datashape=datashape, 
                         dtype=dtype,
                         shape=shape,
                         container=container,
                         get_chunks_supported=False)

    def read(self):
        pass

    def read_chunks(self, chunksize):
        pass

    def get_chunks(self, chunksize):
        return [PostgresSource(**self._init_args)]

    def close(self):
        pass

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
