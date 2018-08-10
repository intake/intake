from . import base


class TextFilesSource(base.DataSource):
    """Read textfiles as sequence of lines

    Protype of sources reading sequential data.

    Takes a set of files, and returns an iterator over the text in each of them.
    The files can be local or remote. Extra parameters for encoding, etc.,
    go into ``storage_options``.
    """
    name = 'textfiles'
    version = '0.0.1'
    container = 'python'
    partition_access = True

    def __init__(self, urlpath, metadata=None,
                 storage_options=None):
        """

        Parameters
        ----------
        urlpath : str or list(str)
            Target files. Can be a glob-path (with "*") and include protocol
            specified (e.g., "s3://"). Can also be a list of absolute paths.
        storage_options: dict
            Options to pass to the file reader backend, including text-specific
            encoding arguments, and parameters specific to the remote
            file-system driver, if using.
        """
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._dataframe = None
        self._files = None

        super(TextFilesSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from dask.bytes import open_files
        if self._files is None:

            urlpath, *_ = self._get_cache(self._urlpath)

            self._files = open_files(urlpath, mode='rt',
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
    """Serializable function to take an OpenFile object and read lines"""
    with f as f:
        return list(f)
