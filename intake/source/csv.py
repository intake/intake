import dask.dataframe
import numpy as np

from . import base


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='csv', version='0.1',
                                     container='dataframe',
                                     partition_access=True)

    def open(self, urlpath, **kwargs):
        storage_options = kwargs.pop('storage_options', None)
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return CSVSource(urlpath=urlpath, csv_kwargs=source_kwargs,
                         metadata=base_kwargs['metadata'],
                         storage_options=storage_options)


# For brevity, this implementation just wraps the Dask dataframe
# implementation. Generally, plugins should not use Dask directly, as the
# base class provides the implementation for to_dask().
class CSVSource(base.DataSource):
    def __init__(self, urlpath, csv_kwargs, metadata, storage_options=None):
        self._urlpath = urlpath
        self._storage_options = storage_options
        self._csv_kwargs = csv_kwargs
        self._dataframe = None

        super(CSVSource, self).__init__(container='dataframe',
                                        metadata=metadata)

    def _get_schema(self):
        if self._dataframe is None:
            self._dataframe = dask.dataframe.read_csv(
                self._urlpath, storage_options=self._storage_options,
                **self._csv_kwargs)

        dtypes = self._dataframe.dtypes
        return base.Schema(datashape=None,
                           dtype=np.dtype(list(zip(dtypes.index, dtypes))),
                           # Shape not known without parsing all the files,
                           # so leave it as 1D unknown
                           shape=(None,),
                           npartitions=self._dataframe.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        return self._dataframe.get_partition(i).compute()

    def _close(self):
        self._dataframe = None
