from intake.container.base import RemoteSource, get_partition
from intake.source.base import Schema


class RemoteSequenceSource(RemoteSource):
    """Sequence-of-things source on an Intake server"""
    name = 'remote_sequence'
    container = 'python'

    def __init__(self, url, headers, **kwargs):
        self.url = url
        self.npartitions = kwargs.get('npartition', 1)
        self.partition_access = self.npartitions > 1
        self.headers = headers
        self.metadata = kwargs.get('metadata', {})
        self._schema = Schema(npartitions=self.npartitions,
                              extra_metadata=self.metadata)
        self.bag = None
        super(RemoteSequenceSource, self).__init__(url, headers, **kwargs)

    def _load_metadata(self):
        import dask.bag as db
        import dask
        if self.bag is None:
            self.parts = [dask.delayed(get_partition)(
                self.url, self.headers, self._source_id, self.container, i
            )
                          for i in range(self.npartitions)]
            self.bag = db.from_delayed(self.parts)
        return self._schema

    def _get_partition(self, i):
        self._load_metadata()
        return self.parts[i].compute()

    def read(self):
        self._load_metadata()
        return self.bag.compute()

    def to_dask(self):
        self._load_metadata()
        return self.bag

    def _close(self):
        self.bag = None
