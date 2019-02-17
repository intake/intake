from .base import DataSource, Schema


class ZarrArraySource(DataSource):
    """Read Zarr format files into an array
    """
    container = 'ndarray'
    name = 'ndzarr'
    version = '0.0.1'
    partition_access = True

    def __init__(self, url, storage_options=None, component=None,
                 metadata=None, **kwargs):
        """
        The parameters dtype and shape will be determined from the first
        file, if not given.

        Parameters
        ----------
        url : str
            Location of data file(s), possibly including and protocol
            information
        storage_options : dict
            Passed on to storage backend for remote files
        component : str or None
            If None, assume the URL points to an array store. If given, assume
            it is a group, and descend the group to find the array at this
            location in the data-set.
        kwargs : passed on to zarr
        """
        self.url = url
        self.storage_options = storage_options or {}
        self.component = component
        self.kwargs = kwargs
        self._arr = None
        super(ZarrArraySource, self).__init__(metadata=metadata)

    def _get_schema(self):
        import dask.array as da
        if self._arr is None:
            self._arr = da.from_zarr(self.url, component=self.component,
                                     storage_options=self.storage_options,
                                     **self.kwargs)
            self.chunks = self._arr.chunks
            self.npartitions = self._arr.npartitions
        return Schema(dtype=str(self.dtype), shape=self.shape,
                      extra_metadata=self.metadata,
                      npartitions=self.npartitions,
                      chunks=self.chunks)

    def _get_partition(self, i):
        if isinstance(i, list):
            i = tuple(i)
        return self._arr.blocks[i].compute()

    def read_partition(self, i):
        self._get_schema()
        return self._get_partition(i)

    def to_dask(self):
        self._get_schema()
        return self._arr

    def read(self):
        self._get_schema()
        return self._arr.compute()

    def _close(self):
        self._arr = None
        self._mapper = None