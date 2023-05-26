"""Classes for reading data into a python object"""

from __future__ import annotations

import importlib.metadata

from intake import import_name
from intake.readers import datatypes
from intake.readers.utils import subclasses


class BaseReader:
    imports = set()  # allow for pip-style versions maybe
    implements = set()
    optional_imports = set()
    func = "builtins:NotImplementedError"
    func_doc = None
    concat_func = None
    output_instance = None

    def __init__(self, data, **kwargs):
        self.data = data
        self.kwargs = kwargs

    @classmethod
    def check_imports(cls):
        """See if required packages are importable, but don't import them"""
        try:
            for package in cls.imports:
                importlib.metadata.distribution(package)
            return True
        except (ImportError, ModuleNotFoundError, NameError):
            return False

    @classmethod
    def output_doc(cls):
        """Doc associated with output type"""
        out = import_name(cls.output_instance)
        return out.__doc__

    @classmethod
    def doc(cls):
        """Doc associated with loading function"""
        f = cls.func_doc or cls.func
        if isinstance(f, str):
            return import_name(f).__doc__
        # if callable
        return f.__doc__

    def discover(self, **kwargs):
        """Part of the data

        The intent is to return a minimal dataset, but for some readers and conditions this may be
        up to the whole of the data. Output type is the same as for read().
        """
        return self.read(**kwargs)

    @property
    def _func(self):
        if isinstance(self.func, str):
            return import_name(self.func)
        return self.func

    @classmethod
    def classname(cls):
        return cls.__name__.lower()

    def read(self, **kwargs):
        """Produce data artefact

        Any of the arguments encoded in the data instance can be overridden.

        Output type is given by the .output_instance attribute
        """
        kw = self.kwargs.copy()
        kw.update(kwargs)
        return self._func(**kw)


class FileReader(BaseReader):
    url_arg = None
    storage_options = False

    def read(self, **kwargs):
        kw = self.kwargs.copy()
        kw.update(kwargs)
        if self.storage_options:
            kw["storage_options"] = self.data.storage_options
        if self.url_arg and self.url_arg not in kwargs and self.concat_func:
            filelist = self.data.filelist
            if len(filelist) > 1:
                concat = import_name(self.concat_func)
                parts = []
                for afile in filelist:
                    kw[self.url_arg] = afile
                    # TODO: can apply file columns here
                    parts.append(self.read(**kw))
                return concat(parts)
        if self.url_arg:
            kw[self.url_arg] = self.data.url

        return self._func(**kw)


class Pandas(FileReader):
    imports = {"pandas"}
    concat_func = "pandas:concat"
    output_instance = "pandas:DataFrame"


class PandasParquet(Pandas):
    concat_func = None  # pandas concats for us
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "pandas:read_parquqet"
    url_arg = "path"


class Dasky(FileReader):
    """Compatibility for dask-producing classes, provides to_dask()->read()"""

    def to_dask(self, **kwargs):
        return self.read(**kwargs)


class DaskParquet(Dasky):
    imports = {"dask", "pandas"}
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "dask.dataframe:read_parquet"
    url_arg = "path"
    output_instance = "dask.dataframe:DataFrame"


class DuckDB(FileReader):
    imports = {"duckdb"}
    output_instance = "duckdb:DuckDBPyRelation"  # can be converted to pandas with .df
    implements = {datatypes.Parquet, datatypes.CSV, datatypes.JSONFile, datatypes.SQLQuery}
    func_doc = "duckdb:query"

    def discover(self, **kwargs):
        return self.read().limit(10)

    def func(self, **kwargs):
        import duckdb

        conn = getattr(self.data, "conn", {})  # only SQL type normally has this
        if isinstance(conn, str):
            conn = {"database": conn}
        self._dd = duckdb.connect(**conn)  # connection must be cached for results to be usable
        queries = {
            datatypes.Parquet: "SELECT * FROM read_parquet('{self.data.url}')",
            datatypes.CSV: "SELECT * FROM read_csv_auto('{self.data.url}')",
            datatypes.JSONFile: "SELECT * FROM read_json_auto('{self.data.url}')",
            datatypes.SQLQuery: "{self.data.query}",
        }
        return self._dd.query(queries[type(self.data)].format(**locals()))


class SparkDataFrame(FileReader):
    imports = {"pyspark"}
    implements = {datatypes.Parquet, datatypes.CSV, datatypes.Text}
    func_doc = "pyspark.sql:SparkSession.read"
    output_instance = "pyspark.sql:DataFrame"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)

    def read(self, **kwargs):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        method_name = {datatypes.CSV: "csv", datatypes.Parquet: "parquet", datatypes.Text: "text"}
        method = getattr(spark.read, method_name[type(self.data)])
        return method(self.data.url, **kwargs)


class Awkward(FileReader):
    imports = {"awkward"}
    output_instance = "awkward:Array"


class AwkwardParquet(Awkward):
    # TODO: merge JSON and/or root into here?
    implements = {datatypes.Parquet}
    imports = {"awkward", "pyarrow"}
    func = "awkward:from_parquet"
    url_arg = "path"

    def discover(self, **kwargs):
        kwargs["row_groups"] = [0]
        return self.read(**kwargs)


class DaskAwkwardParquet(AwkwardParquet, Dasky):
    imports = {"dask_awkward", "pyarrow"}
    func = "dask_awkward:from_parquet"
    output_instance = "dask_awkward:Array"

    def discover(self, **kwargs):
        return self.read(**kwargs).partitions[0]


class PandasCSV(Pandas):
    implements = {datatypes.CSV}
    func = "pandas:read_csv"
    url_arg = "filepath_or_buffer"

    def discover(self, **kwargs):
        kw = {"nrows": 10, self.url_arg: self.data.filelist[0]}
        kw.update(kwargs)
        return self.read(**kw)


class Ray(FileReader):
    # https://docs.ray.io/en/latest/data/creating-datasets.html#supported-file-formats
    implements = {datatypes.CSV, datatypes.Parquet, datatypes.JSONFile, datatypes.Text}
    imports = {"ray"}
    output_instance = {"ray.data:Dataset"}
    func_doc = {"ray.data:Dataset"}
    url_arg = "paths"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)

    def read(self, **kwargs):
        data = import_name("ray.data")
        method_name = {datatypes.CSV: "read_csv", datatypes.JSONFile: "read_json", datatypes.Parquet: "read_parquet", datatypes.Text: "read_text"}[type(self.data)]
        method = getattr(data, method_name)
        kwargs[self.url_arg] = self.data.url
        return method(**kwargs)


def recommend(data):
    """Show which readers claim to support the given data instance"""
    out = {"importable": set(), "not_importable": set()}
    for cls in subclasses(BaseReader):
        if type(data) in cls.implements:
            if cls.check_imports():
                out["importable"].add(cls)
            else:
                out["not_importable"].add(cls)
    return out
