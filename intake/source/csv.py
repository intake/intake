# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from fsspec.core import get_fs_token_paths

from . import base
from .utils import reverse_formats, unique_string


class CSVSource(base.DataSource, base.PatternMixin):
    """Read CSV files into dataframes

    Prototype of sources reading dataframe data

    """

    name = "csv"
    version = "0.0.1"
    container = "dataframe"
    partition_access = True

    def __init__(self, urlpath, csv_kwargs=None, metadata=None, storage_options=None, path_as_pattern=True):
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
        self._storage_options = storage_options or {}
        self._csv_kwargs = csv_kwargs or {}
        self._dask_df = None
        self._files = None
        self._schema = None

        super(CSVSource, self).__init__(metadata=metadata)

    def _set_pattern_columns(self, df, path_column):
        """Get a column of values for each field in pattern"""
        from pandas.api.types import CategoricalDtype

        col = df[path_column]
        paths = sorted(col.cat.categories)

        column_by_field = {
            field: col.cat.codes.map(dict(enumerate(values))).astype(CategoricalDtype(set(values))) for field, values in reverse_formats(self.pattern, paths).items()
        }

        return df.assign(**column_by_field)

    def _path_column(self):
        """Set ``include_path_column`` in csv_kwargs and returns path column name"""
        path_column = self._csv_kwargs.get("include_path_column")

        if path_column is None:
            # if path column name is not set by user, set to a unique string to
            # avoid conflicts
            path_column = unique_string()
            self._csv_kwargs["include_path_column"] = path_column
        elif isinstance(path_column, bool):
            path_column = "path"
            self._csv_kwargs["include_path_column"] = path_column
        return path_column

    def _open_dask(self):
        """Open dataset using dask and use pattern fields to set new columns"""

        if self._dask_df is not None:
            return

        import dask.dataframe

        urlpath = self._get_cache(self._urlpath)[0]

        if self.pattern is None:
            self._dask_df = dask.dataframe.read_csv(urlpath, storage_options=self._storage_options, **self._csv_kwargs)
            return

        drop_path_column = "include_path_column" not in self._csv_kwargs
        path_column = self._path_column()

        self._dask_df = dask.dataframe.read_csv(urlpath, storage_options=self._storage_options, **self._csv_kwargs)

        # add the new columns to the dataframe
        self._dask_df = self._set_pattern_columns(self._dask_df, path_column)

        if drop_path_column:
            self._dask_df = self._dask_df.drop([path_column], axis=1)

    def files(self):
        if self._files is not None:
            return self._files

        urlpath = self._get_cache(self._urlpath)[0]
        if not isinstance(urlpath, list):
            urlpath = [urlpath]

        glob_in_path = any("*" in path for path in urlpath)
        if self.pattern is None and not glob_in_path:
            self._files = urlpath
        else:
            self._files = get_fs_token_paths(urlpath)[2]

        return sorted(self._files)

    def _get_schema(self):
        if self._schema is not None:
            return self._schema

        if self._dask_df is not None:
            dtypes = self._dask_df._meta.dtypes.to_dict()
            dtypes = {n: str(t) for (n, t) in dtypes.items()}
            self._schema = base.Schema(dtype=dtypes, shape=(None, len(dtypes)), npartitions=self._dask_df.npartitions, extra_metadata={})
            return self._schema

        nrows = self._csv_kwargs.get("nrows")
        self._csv_kwargs["nrows"] = 10
        df = self._get_partition(0)
        if nrows is None:
            del self._csv_kwargs["nrows"]
        else:
            self._csv_kwargs["nrows"] = nrows

        dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
        self._schema = base.Schema(dtype=dtypes, shape=(None, len(dtypes)), npartitions=len(self.files()), extra_metadata={})

        return self._schema

    def _get_partition(self, i):
        if self._dask_df is not None:
            # FIX: side effects from having different partition
            # definitions (dask vs. files)?
            return self._dask_df.get_partition(i).compute()

        url_part = self.files()[i]
        return self._read_pandas(url_part, i)

    def _read_pandas(self, url_part, i):
        import pandas as pd

        if self.pattern is None:
            return pd.read_csv(url_part, storage_options=self._storage_options, **self._csv_kwargs)

        drop_path_column = "include_path_column" not in self._csv_kwargs
        path_column = self._path_column()

        csv_kwargs = self._csv_kwargs
        csv_kwargs.pop("include_path_column")
        df_part = pd.read_csv(url_part, storage_options=self._storage_options, **csv_kwargs)

        from pandas.api.types import CategoricalDtype

        df_part[path_column] = pd.Series(url_part, index=df_part.index, dtype=CategoricalDtype(self.files()))
        df_part = self._set_pattern_columns(df_part, path_column)
        if drop_path_column:
            df_part = df_part.drop([path_column], axis=1)

        return df_part

    def read(self):
        if self._dask_df is not None:
            return self._dask_df.compute()

        import pandas as pd

        self._get_schema()
        return pd.concat([self._get_partition(i) for i in range(len(self.files()))])

    def to_dask(self):
        self._open_dask()
        return self._dask_df

    def to_spark(self):
        from intake_spark.base import SparkHolder

        h = SparkHolder(True, [("read",), ("format", ("csv",)), ("option", ("header", "true")), ("load", (self.urlpath,))], {})
        return h.setup()

    def _close(self):
        self._dask_df = None
        self._files = None
