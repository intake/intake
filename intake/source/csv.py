import dask.dataframe as dd

from . import base


class Plugin(base.Plugin):
    def __init__(self):
        super().__init__(name='csv', version='0.1', container='dataframe', partition_access=True)

    def open(self, urlpath, **kwargs):
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return CSVSource(urlpath=urlpath, csv_kwargs=source_kwargs, metadata=base_kwargs['metadata'])


class CSVSource(base.DataSource):
    def __init__(self, urlpath, csv_kwargs, metadata):
        self._init_args = dict(urlpath=urlpath, csv_kwargs=csv_kwargs, metadata=metadata)

        self._urlpath = urlpath
        self._csv_kwargs = csv_kwargs
        self._dataframe = None

        super().__init__(container='dataframe', metadata=metadata)

    def _get_dataframe(self):
        if self._dataframe is None:
            self._dataframe = dd.read_csv(self._urlpath, **self._csv_kwargs)

            dtypes = self._dataframe.dtypes
            self.datashape = None
            self.dtype = list(zip(dtypes.index, dtypes))
            self.shape = (len(self._dataframe),)
            self.npartitions = self._dataframe.npartitions

        return self._dataframe

    def discover(self):
        self._get_dataframe()
        return dict(datashape=self.datashape, dtype=self.dtype, shape=self.shape, npartitions=self.npartitions)

    def read(self):
        return self._get_dataframe().compute()

    def read_chunked(self):
        df = self._get_dataframe()

        for i in range(df.npartitions):
            yield df.get_partition(i).compute()

    def read_partition(self, i):
        df = self._get_dataframe()
        return df.get_partition(i).compute()

    def to_dask(self):
        return self._get_dataframe()

    def close(self):
        self._dataframe = None

    def __getstate__(self):
        return self._init_args

    def __setstate__(self, state):
        self.__init__(**state)
