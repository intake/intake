from intake.source.base import Schema
from .base import RemoteSource, get_partition


class RemoteDataFrame(RemoteSource):

    name = 'remote_dataframe'
    container = 'dataframe'

    def __init__(self, url, headers, **kwargs):
        import pickle
        super(RemoteDataFrame, self).__init__(url, headers, **kwargs)
        self.npartitions = kwargs['npartitions']
        self.shape = tuple(kwargs['shape'])
        self.metadata = kwargs['metadata']
        d = kwargs['dtype']
        if isinstance(d, bytes):
            self.dtype = pickle.loads(d)
        else:
            self.dtype = d
        self._schema = Schema(npartitions=self.npartitions,
                              extra_metadata=self.metadata,
                              dtype=self.dtype,
                              shape=self.shape,
                              datashape=None)
        self.dataframe = None

    def _load_metadata(self):
        import dask.dataframe as dd
        import dask
        if self.dataframe is None:
            self.parts = [dask.delayed(get_partition)(
                self.url, self.headers, self._source_id, self.container, i
            )
                          for i in range(self.npartitions)]
            self.dataframe = dd.from_delayed(self.parts)
        return self._schema

    def _get_partition(self, i):
        self._load_metadata()
        return self.parts[i].compute()

    def read(self):
        self._load_metadata()
        return self.dataframe.compute()

    def to_dask(self):
        self._load_metadata()
        return self.dataframe

    def _close(self):
        self.dataframe = None
