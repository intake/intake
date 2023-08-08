"""Classes for reading data into a python object"""

from __future__ import annotations

import importlib.metadata
import inspect
from itertools import chain

from intake import import_name
from intake.readers import datatypes
from intake.readers.utils import Tokenizable, find_funcs, subclasses


class BaseReader(Tokenizable):
    imports: set[str] = set()  # allow for pip-style versions maybe
    implements: set[datatypes.BaseData] = set()
    optional_imports: set[str] = set()
    func: str = "builtins:NotImplementedError"
    func_doc: str = None
    concat_func: str = None
    output_instance: str = None

    def __init__(self, data, metadata: dict | None = None, **kwargs):
        """

        Parameters
        ----------
        data: intake.readers.datatypes.BaseData
        """
        self.data = data
        self.kwargs = kwargs
        self.metadata = metadata or {}

    def __repr__(self):
        return f"{type(self).__name__} reader for {self.data} producing {self.output_instance}"

    def __call__(self, **kwargs):
        """New version of this instance with altered arguments"""
        kw = self.kwargs.copy()
        kw.update(kwargs)
        return type(self)(self.data, **kw)

    def __getattr__(self, item):
        if item in self._namespaces:
            return self._namespaces[item]
        return self.transform.__getattr__(item)

    def __getitem__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import getitem

        outtype = self.output_instance
        func = getitem
        if isinstance(self, Pipeline):
            return self.with_step((func, {"item": item}), out_instance=outtype)

        return Pipeline(data=datatypes.ReaderData(reader=self), steps=[(func, {"item": item})], out_instances=[outtype])

    def __dir__(self):
        return list(sorted(chain(object.__dir__(self), dir(self.transform), self._namespaces)))

    @property
    def _namespaces(self):
        from intake.readers.namespaces import get_namespaces

        return get_namespaces(self)

    def clone_new(self, **kwargs):
        """Compatibility method from intake 1.0's Source"""
        return self(**kwargs)

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
        """Import and replace .func, if it is a string"""
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

    def apply(self, func, output_instance=None, **kwargs):
        """Make a pipeline by applying a function to this reader's output"""
        from intake.readers.convert import Pipeline

        return Pipeline(datatypes.ReaderData(reader=self), [(func, kwargs)], [output_instance or self.output_instance])

    @property
    def transform(self):
        from intake.readers.convert import convert_funcs

        funcdict = convert_funcs(self.output_instance)
        return Functioner(self, funcdict)

    def to_entry(self):
        """Create an entry with only this reader defined"""
        from intake.readers.entry import ReaderDescription

        return ReaderDescription(data=self.data.to_entry(), reader=self.qname(), kwargs=find_funcs(self.kwargs))


class Functioner:
    """Find and apply transform functions to reader output"""

    def __init__(self, reader, funcdict):
        self.reader = reader
        self.funcdict = funcdict

    def _ipython_key_completions_(self):
        return list(self.funcdict)

    def __getitem__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import getitem

        if item in self.funcdict:
            func = self.funcdict[item]
            kw = {}
        else:
            func = getitem
            kw = {"item": item}
        if isinstance(self.reader, Pipeline):
            return self.reader.with_step((func, kw), out_instance=item)

        return Pipeline(data=datatypes.ReaderData(reader=self.reader), steps=[(func, kw)], out_instances=[item])

    def __repr__(self):
        import pprint

        # TODO: replace .*/SameType outputs with out output_instance
        return f"Transformers for {self.reader.output_instance}:\n{pprint.pformat(self.funcdict)}"

    def __dir__(self):
        return list(sorted(f.__name__ for f in self.funcdict.values()))

    def __getattr__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import method

        out = [(outtype, func) for outtype, func in self.funcdict.items() if func.__name__ == item]
        if not len(out):
            outtype = self.reader.output_instance
            func = method
            kw = {"method_name": item}
        else:
            outtype, func = out[0]
            kw = {}
        if isinstance(self.reader, Pipeline):
            return self.reader.with_step((func, kw), out_instance=outtype)

        return Pipeline(data=datatypes.ReaderData(reader=self.reader), steps=[(func, kw)], out_instances=[outtype])


class FileReader(BaseReader):
    """Convenience superclass for readers of files"""

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


class FileByteReader(FileReader):
    """The contents of file(s) as bytes objects"""

    output_instance = "builtin:bytes"
    implements = {datatypes.FileData}

    def discover(self, **kwargs):
        import fsspec

        with fsspec.open(self.data.url, mode="rb", **(self.data.storage_options or {})) as f:
            return f.read()

    def read(self, **kwargs):
        import fsspec

        out = []
        for of in fsspec.open_files(self.data.url, mode="rb", **(self.data.storage_options or {})):
            with of as f:
                out.append(f.read())
        return b"".join(out)


class Pandas(FileReader):
    imports = {"pandas"}
    concat_func = "pandas:concat"
    output_instance = "pandas:DataFrame"
    storage_options = True


class PandasParquet(Pandas):
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "pandas:read_parquet"
    url_arg = "path"


class DaskDF(FileReader):
    imports = {"dask", "pandas"}
    output_instance = "dask.dataframe:DataFrame"

    def to_dask(self, **kwargs):
        return self.read(**kwargs)


class DaskParquet(DaskDF):
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "dask.dataframe:read_parquet"
    url_arg = "path"


class DuckDB(BaseReader):
    imports = {"duckdb"}
    output_instance = "duckdb:DuckDBPyRelation"  # can be converted to pandas with .df
    func_doc = "duckdb:query"
    _dd = {}  # hold the engines, so results are still valid

    def discover(self, **kwargs):
        return self.read().limit(10)

    def _duck(self):
        import duckdb

        conn = getattr(self.data, "conn", {})  # only SQL type normally has this
        if conn not in self._dd:
            if isinstance(conn, str):
                conn = {"database": conn}
            self._dd[conn] = duckdb.connect(**conn)  # connection must be cached for results to be usable
        return self._dd[conn]


class DuckParquet(DuckDB, FileReader):
    implements = {datatypes.Parquet}

    def read(self, **kwargs):
        return self._duck().query(f"SELECT * FROM read_parquet('{self.data.url}')")


class DuckCSV(DuckDB, FileReader):
    implements = {datatypes.CSV}

    def read(self, **kwargs):
        return self._duck().query(f"SELECT * FROM read_csv_auto('{self.data.url}')")


class DuckJSON(DuckDB, FileReader):
    implements = {datatypes.JSONFile}

    def read(self, **kwargs):
        return self._duck().query(f"SELECT * FROM read_json_auto('{self.data.url}')")


class DuckSQL(DuckDB):
    implements = {datatypes.SQLQuery}

    def read(self, **kwargs):
        return self._duck().query(self.data.query)


class SparkDataFrame(FileReader):
    imports = {"pyspark"}
    func_doc = "pyspark.sql:SparkSession.read"
    output_instance = "pyspark.sql:DataFrame"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)

    def _spark(self):
        SparkSession = import_name("pyspark.sq:SparkSession")
        return SparkSession.builder.getOrCreate()


class SparkCSV(SparkDataFrame):
    implements = {datatypes.CSV}

    def read(self, **kwargs):
        return self._spark().read.csv(self.data.url, **kwargs)


class SparkParquet(SparkDataFrame):
    implements = {datatypes.Parquet}

    def read(self, **kwargs):
        return self._spark().read.parquet(self.data.url, **kwargs)


class SparkText(SparkDataFrame):
    implements = {datatypes.Text}

    def read(self, **kwargs):
        return self._spark().read.text(self.data.url, **kwargs)


class Awkward(FileReader):
    imports = {"awkward"}
    output_instance = "awkward:Array"


class AwkwardParquet(Awkward):
    implements = {datatypes.Parquet}
    imports = {"awkward", "pyarrow"}
    func = "awkward:from_parquet"
    url_arg = "path"

    def discover(self, **kwargs):
        kwargs["row_groups"] = [0]
        return self.read(**kwargs)


class DaskAwkwardParquet(AwkwardParquet, DaskDF):
    imports = {"dask_awkward", "pyarrow"}
    func = "dask_awkward:from_parquet"
    output_instance = "dask_awkward:Array"

    def discover(self, **kwargs):
        return self.read(**kwargs).partitions[0]


class AwkwardJSON(Awkward):
    implements = {datatypes.JSONFile}
    func = "awkward:from_json"
    url_arg = "source"


class DaskAwkwardJSON(Awkward, DaskDF):
    imports = {"dask_awkward"}
    func = "dask_awkward:from_json"
    output_instance = "dask_awkward:Array"
    url_arg = "source"

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


class DaskCSV(DaskDF):
    implements = {datatypes.CSV}
    func = "dask.dataframe:read_csv"
    url_arg = "urlpath"


class Ray(FileReader):
    # https://docs.ray.io/en/latest/data/creating-datasets.html#supported-file-formats
    imports = {"ray"}
    output_instance = "ray.data:Dataset"
    url_arg = "paths"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)


class RayParquet(Ray):
    implements = {datatypes.Parquet}
    func = "ray.data:read_parquet"


class RayCSV(Ray):
    implements = {datatypes.CSV}
    func = "ray.data:read_csv"


class RayJSON(Ray):
    implements = {datatypes.JSONFile}
    func = "ray.data:read_json"


class RayText(Ray):
    implements = {datatypes.Text}
    func = "ray.data:read_text"


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
        if any(isinstance(data, imp) for imp in cls.implements):
            if cls.check_imports():
                out["importable"].add(cls)
            else:
                out["not_importable"].add(cls)
    return out


def reader_from_call(func, *args, **kwargs):
    """Attempt to construct a reader instance by finding one that matches the function call

    Fails for readers that don't define a func, probably because it depends on the file
    type or needs a dynamic instance to be a method of.

    Parameters
    ----------
    func: callable | str
        If a callable, pass args and kwargs as you would have done to execute the function.
        If a string, it should look like "func(arg1, args2, kwarg1, **kw)", i.e., a normal
        python call but as a string. In the latter case, args and kwargs are ignored
    """

    import re

    if isinstance(func, str):
        frame = inspect.currentframe().f_back
        match = re.match("^(.*?)[(](.*)[)]", func)
        if match:
            groups = match.groups()
        else:
            raise ValueError
        func = eval(groups[0], frame.f_globals, frame.f_locals)
        args, kwargs = eval(f"""(lambda *args, **kwargs: (args, kwargs))({groups[1]})""", frame.f_globals, frame.f_locals)

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
        data_kw["storage_options"] = kw.pop("storage_options", None)
        data_kw["url"] = kw.pop(cls.url_arg)
    datacls = None
    if len(cls.implements) == 1:
        datacls = next(iter(cls.implements))
    elif cls.url_arg:
        clss = datatypes.recommend(kwargs[cls.url_arg], storage_options=(data_kw["url"] if cls.storage_options is not None else None))
        if clss:
            datacls = next(iter(clss))
    if datacls:
        datacls = datacls(**data_kw)
        cls = cls(datacls, **kwargs)

    return {"reader": cls, "kwargs": kw, "data": datacls}
