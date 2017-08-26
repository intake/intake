import glob

import dask.dataframe as dd

from . import base


class Plugin(base.Plugin):
    def __init__(self):
        pass

    def open(self, urlpath, **kwargs):
        return CSVSource(urlpath=urlpath, csv_kwargs=kwargs)


class CSVSource(base.DataSource):
    def __init__(self, urlpath, csv_kwargs):
        self._init_args = dict(urlpath=urlpath, csv_kwargs=csv_kwargs)

        self._urlpath = urlpath
        self._csv_kwargs = csv_kwargs
        self._dataframe = None

        super().__init__(container='dataframe')

    def _get_dataframe(self):
        if self._dataframe is None:
            self._dataframe = dd.read_csv(self._urlpath, **self._csv_kwargs)

            dtypes = self._dataframe.dtypes
            self.datashape = None
            self.dtype = list(zip(dtypes.index, dtypes))
            self.shape = (len(self._dataframe),)

        return self._dataframe

    def discover(self):
        self._get_dataframe()

    def read(self):
        return self._get_dataframe().compute()

    def read_chunks(self):
        df = self._get_dataframe()

        for i in range(df.npartitions):
            yield df.get_partition.compute()

    def to_dask(self):
        return self._get_dataframe()

    def close(self):
        self._dataframe = None

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
