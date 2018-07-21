from . import base


class TextFilesSource(base.DataSource):
    """Read textfiles as sequence of lines
    """
    name = 'textfiles'
    version = '0.0.1'
    container = 'python'
    partition_access = True

    def __init__(self, urlpath, metadata=None,
                 storage_options=None):
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._dataframe = None
        self._files = None

        super(TextFilesSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from dask.bytes import open_files
        if self._files is None:
            self._files = open_files(self._urlpath, mode='rt',
                                     **self._storage_options)
            self.npartitions = len(self._files)
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=(None, ),
                           npartitions=self.npartitions,
                           extra_metadata=self.metadata)

    def _get_partition(self, i):
        return get_file(self._files[i])

    def read(self):
        self._get_schema()
        return self.to_dask().compute()

    def to_dask(self):
        import dask.bag as db
        from dask import delayed
        dfile = delayed(get_file)
        return db.from_delayed([dfile(f) for f in self._files])


def get_file(f):
    with f as f:
        return list(f)
