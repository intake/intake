#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from . import base
from .utils import DASK_VERSION, reverse_formats, unique_string


class CSVSource(base.DataSource, base.PatternMixin):
    """Read CSV files into dataframes

    Prototype of sources reading dataframe data

    """
    name = 'csv'
    version = '0.0.1'
    container = 'dataframe'
    partition_access = True

    def __init__(self, urlpath, csv_kwargs=None, metadata=None,
                 storage_options=None, path_as_pattern=True):
        """
        Parameters
        ----------
        urlpath : str or iterable, location of data
            May be a local path, or remote path if including a protocol specifier
            such as ``'s3://'``. May include glob wildcards or format pattern strings.
            Some examples:

            - ``{{ CATALOG_DIR }}data/precipitation.csv``
            - ``s3://data/*.csv``
            - ``s3://data/precipitation_{state}_{zip}.csv``
            - ``s3://data/{year}/{month}/{day}/precipitation.csv``
            - ``{{ CATALOG_DIR }}data/precipitation_{date:%Y-%m-%d}.csv``
        csv_kwargs : dict
            Any further arguments to pass to Dask's read_csv (such as block size)
            or to the `CSV parser <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html>`_
            in pandas (such as which columns to use, encoding, data-types)
        storage_options : dict
            Any parameters that need to be passed to the remote data backend,
            such as credentials.
        path_as_pattern : bool or str, optional
            Whether to treat the path as a pattern (ie. ``data_{field}.csv``)
            and create new columns in the output corresponding to pattern
            fields. If str, is treated as pattern to match on. Default is True.
        """
        self.path_as_pattern = path_as_pattern
        self.urlpath = urlpath
        self._storage_options = storage_options
        self._csv_kwargs = csv_kwargs or {}
        self._dataframe = None

        super(CSVSource, self).__init__(metadata=metadata)

    def _set_pattern_columns(self, path_column):
        """Get a column of values for each field in pattern
        """
        from pandas.api.types import CategoricalDtype

        col = self._dataframe[path_column]
        paths = sorted(col.cat.categories)

        column_by_field = {field:
            col.cat.codes.map(dict(enumerate(values))).astype(
                CategoricalDtype(set(values))
            ) for field, values in reverse_formats(self.pattern, paths).items()
        }
        self._dataframe = self._dataframe.assign(**column_by_field)

    def _path_column(self):
        """Set ``include_path_column`` in csv_kwargs and returns path column name
        """
        path_column = self._csv_kwargs.get('include_path_column')

        if path_column is None:
            # if path column name is not set by user, set to a unique string to
            # avoid conflicts
            path_column = unique_string()
            self._csv_kwargs['include_path_column'] = path_column
        elif isinstance(path_column, bool):
            path_column = 'path'
            self._csv_kwargs['include_path_column'] = path_column
        return path_column

    def _open_dataset(self, urlpath):
        """Open dataset using dask and use pattern fields to set new columns
        """
        import dask.dataframe

        if self.pattern is None:
            self._dataframe = dask.dataframe.read_csv(
                urlpath, storage_options=self._storage_options,
                **self._csv_kwargs)
            return

        drop_path_column = 'include_path_column' not in self._csv_kwargs
        path_column = self._path_column()

        self._dataframe = dask.dataframe.read_csv(
            urlpath, storage_options=self._storage_options, **self._csv_kwargs)

        # add the new columns to the dataframe
        self._set_pattern_columns(path_column)

        if drop_path_column:
            self._dataframe = self._dataframe.drop([path_column], axis=1)

    def _get_schema(self):
        urlpath = self._get_cache(self._urlpath)[0]

        if self._dataframe is None:
            self._open_dataset(urlpath)

        dtypes = self._dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return base.Schema(dtype=dtypes,
                           shape=(None, len(dtypes)),
                           npartitions=self._dataframe.npartitions,
                           extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def to_spark(self):
        from intake_spark.base import SparkHolder
        h = SparkHolder(True, [
            ('read', ),
            ('format', ("csv", )),
            ('option', ("header", "true")),
            ('load', (self.urlpath, ))
        ], {})
        return h.setup()

    def _close(self):
        self._dataframe = None
