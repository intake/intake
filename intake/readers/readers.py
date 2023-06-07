"""Classes for reading data into a python object"""

from __future__ import annotations

import importlib.metadata
import inspect

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

    def __init__(self, data, entry=None, **kwargs):
        self.data = data
        self.kwargs = kwargs
        self.entry = entry

    def __call__(self, outtype=None, reader=None, **kwargs):
        if self.entry:
            return self.entry.get_reader(outtype=outtype, reader=reader, **kwargs)
        else:
            return self.to_entry().get_reader(outtype=outtype, reader=reader, **kwargs)

    def clone_new(self, outtype=None, reader=None, **kwargs):
        return self(outtype=None, reader=None, **kwargs)

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
        return self._func(self.data, **kw)

    @property
    def transform(self):
        from intake.readers.convert import convert_funcs

        funcdict = convert_funcs(self.output_instance)
        return Functioner(self, funcdict)

    def to_entry(self):
        from intake.readers.entry import DataDescription

        return DataDescription(self.data, {type(self).__name__.lower(): self.kwargs})


class Functioner:
    """Find and apply transform functions to reader output"""

    def __init__(self, reader, funcdict):
        self.reader = reader
        self.funcdict = funcdict

    def _ipython_key_completions_(self):
        return list(self.funcdict)

    def __getitem__(self, item):
        from intake.readers.convert import ConvertReader

        func = self.funcdict[item]
        return ConvertReader(self.reader, func, output_instance=item)

    def __repr__(self):
        return f"Transformers for {self.reader.output_instance}:\n{self.funcdict}"

    def __dir__(self):
        return list(sorted(f.__name__ for f in self.funcdict.values()))

    def __getattr__(self, item):
        from intake.readers.convert import ConvertReader

        out = [(outtype, func) for outtype, func in self.funcdict.items() if func.__name__ == item]
        if len(out):
            outtype, func = out[0]
            return ConvertReader(self.reader, func, output_instance=outtype)
        raise KeyError(item)


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
    concat_func = "pandas:concat"
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "pandas:read_parquet"
    url_arg = "path"
    storage_options = True


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
    _dd = {}  # hold the engines, so results are still valid

    def discover(self, **kwargs):
        return self.read().limit(10)

    def func(self, **kwargs):
        import duckdb

        conn = getattr(self.data, "conn", {})  # only SQL type normally has this
        if isinstance(conn, str):
            conn = {"database": conn}
        self._dd[conn] = duckdb.connect(**conn)  # connection must be cached for results to be usable
        queries = {
            datatypes.Parquet: "SELECT * FROM read_parquet('{self.data.url}')",
            datatypes.CSV: "SELECT * FROM read_csv_auto('{self.data.url}')",
            datatypes.JSONFile: "SELECT * FROM read_json_auto('{self.data.url}')",
            datatypes.SQLQuery: "{self.data.query}",
        }
        return self._dd[0].query(queries[type(self.data)].format(**locals()))


class SparkDataFrame(FileReader):
    imports = {"pyspark"}
    implements = {datatypes.Parquet, datatypes.CSV, datatypes.Text}
    func_doc = "pyspark.sql:SparkSession.read"
    output_instance = "pyspark.sql:DataFrame"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)

    def read(self, **kwargs):
        SparkSession = import_name("pyspark.sq:SparkSession")

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


class DaskCSV(Dasky):
    implements = {datatypes.CSV}
    func = "dask.dataframe:read_csv"
    url_arg = "urlpath"
    output_instance = "dask.dataframe:DataFrame"


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


class TiledNode(BaseReader):
    implements = {datatypes.CatalogAPI}
    imports = {"tiled"}
    # a Node can convert to a Catalog
    output_instance = {"tiled.client.node:Node"}
    func = "tiled.client:from_uri"

    def read(self, **kwargs):
        return self._func(self.data.api_root, **kwargs)


class TiledDataset(BaseReader):
    # returns dask/normal xarray or dataframe
    implements = {datatypes.Tiled}
    output_instance = "tiled.client.base:BaseClient"


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


def reader_from_call(func, *args, **kwargs):
    package = func.__module__.split(".", 1)[0]

    found = False
    for cls in subclasses(BaseReader):
        if callable(cls.func):
            if cls.func == func:
                found = cls
                break
        elif cls.check_imports() and cls.func.split(":", 1)[0].split(".", 1)[0] == package:
            if import_name(cls.func) == func:
                found = cls
                break
    if not found:
        raise ValueError("Function not found in the set of readers")

    pars = inspect.signature(func).parameters
    kw = dict(zip(pars, args), **kwargs)
    # TODO: unwrap args to make datatype class; url/storage_options are easy but what about others?
    #  For a reader implementing multiple types, can we know which one - guess from URL, if given?
    data_kw = {}
    if issubclass(cls, FileReader):
        data_kw["storage_options"] = kw.pop(cls.storage_options, None)
        data_kw["url"] = kw.pop(cls.url_arg)
    datacls = None
    if len(cls.implements) == 1:
        datacls = next(iter(cls.implements))
    elif cls.url_arg:
        clss = datatypes.recommend(kwargs[cls.url_arg], storage_options=(data_kw["url"] if cls.storage_options else None))
        if clss:
            datacls = next(iter(clss))
    if datacls:
        datacls = datacls(**data_kw)
        cls = cls(datacls, **kwargs)

    return {"reader": cls, "kwargs": kw, "data": datacls}
