#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from intake.source.base import Schema, DataSource
import os
import posixpath
from .base import RemoteSource, get_partition
from ..config import conf


class RemoteDataFrame(RemoteSource):
    """Dataframe on an Intake server"""

    name = 'remote_dataframe'
    container = 'dataframe'

    def __init__(self, url, headers, **kwargs):
        super(RemoteDataFrame, self).__init__(url, headers, **kwargs)
        self.npartitions = kwargs['npartitions']
        self.shape = tuple(kwargs['shape'])
        self.metadata = kwargs['metadata']
        self.dtype = kwargs['dtype']
        self._schema = Schema(npartitions=self.npartitions,
                              extra_metadata=self.metadata,
                              dtype=self.dtype,
                              shape=self.shape,
                              datashape=None)
        self.dataframe = None

    def _load_metadata(self):
        import dask.dataframe as dd
        import dask.delayed
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

    @staticmethod
    def _persist(source, path, **kwargs):
        """Save dataframe to local persistent store

        Makes a parquet dataset out of the data using dask.dataframe.to_parquet.
        This then becomes a data entry in the persisted datasets catalog.

        Parameters
        ----------
        source: a DataSource instance to save
        name: str or None
            Key to refer to this persisted dataset by. If not given, will
            attempt to get from the source's name
        kwargs: passed on to dask.dataframe.to_parquet
        """
        try:
            from intake_parquet import ParquetSource
        except ImportError:
            raise ImportError("Please install intake-parquet to use persistence"
                              " on dataframe container sources.")
        try:
            df = source.to_dask()
        except NotImplementedError:
            import dask.dataframe as dd
            df = dd.from_pandas(source.read(), 1)
        df.to_parquet(path, **kwargs)
        source = ParquetSource(path, meta={})
        return source


class GenericDataFrame(DataSource):
    """Create partitioned dataframe from a set of files and any reader

    This data-source allows you to specify any reader to create dataframes.
    The reader must take an open file-like object as input, and output a
    pandas.DataFrame.

    Parameters
    ----------
    urlpath: str
        Location of data. May be local files or remote with a protocol
        specifier. May be a list of files or a glob pattern to be expanded.
    reader: func
        f(open_file, **kwargs) -> pandas.DataFrame
    storage_options: dict
        Passed to the file-system backend to open files; typically includes
        credentials for remote storage
    kwargs:
        Passed to reader function
    """

    name = 'generic_dataframe'
    container = 'dataframe'

    def __init__(self, urlpath, reader, storage_options=None, **kwargs):
        self.url = urlpath
        self.reader = reader
        self.storage_options = storage_options or {}
        kwargs = kwargs.copy()
        super().__init__(metadata=kwargs.pop('metadata', {}))
        self.kwargs = kwargs
        self.dataframe = None

    def _load_metadata(self):
        import dask.dataframe as dd
        import dask.delayed
        from dask.bytes import open_files
        self.files = open_files(self.url, **self.storage_options)

        def read_a_file(open_file, reader, kwargs):
            with open_file as of:
                df = reader(of, **kwargs)
                df['path'] = open_file.path
                return df

        if self.dataframe is None:
            self.parts = [
                dask.delayed(read_a_file)(open_file, self.reader, self.kwargs)
                for open_file in self.files
            ]
            self.dataframe = dd.from_delayed(self.parts)
            self.npartitions = self.dataframe.npartitions
            self.shape = (None, len(self.dataframe.columns))
            self.dtype = self.dataframe.dtypes.to_dict()
            self._schema = Schema(npartitions=self.npartitions,
                                  extra_metadata=self.metadata,
                                  dtype=self.dtype,
                                  shape=self.shape,
                                  datashape=None)
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
