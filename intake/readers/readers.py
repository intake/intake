"""Classes for reading data into a python object"""

from __future__ import annotations

import importlib.metadata
import inspect
import re

import fsspec

from intake import import_name
from intake.readers import datatypes
from intake.readers.mixins import PipelineMixin
from intake.readers.utils import Tokenizable, find_funcs, subclasses


class BaseReader(Tokenizable, PipelineMixin):
    imports: set[str] = set()  # allow for pip-style versions maybe
    implements: set[datatypes.BaseData] = set()
    optional_imports: set[str] = set()
    func: str = "builtins:NotImplementedError"
    func_doc: str = None
    concat_func: str = None
    output_instance: str = None

    def __init__(self, data, metadata: dict | None = None, output_instance: str | None = None, **kwargs):
        """

        Parameters
        ----------
        data: intake.readers.datatypes.BaseData
        """
        self.data = data
        self.kwargs = kwargs
        self.metadata = metadata or {}
        if output_instance:
            self.output_instance = output_instance

    def __repr__(self):
        return f"{type(self).__name__} reader for {self.data} producing {self.output_instance}"

    def __call__(self, **kwargs):
        """New version of this instance with altered arguments"""
        kw = self.kwargs.copy()
        kw.update(kwargs)
        return type(self)(self.data, **kw)

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

    def read(self, **kwargs):
        """Produce data artefact

        Any of the arguments encoded in the data instance can be overridden.

        Output type is given by the .output_instance attribute
        """
        kw = self.kwargs.copy()
        kw.update(kwargs)
        return self._func(self.data, **kw)

    def to_entry(self):
        """Create an entry with only this reader defined"""
        from intake.readers.entry import ReaderDescription

        return ReaderDescription(data=self.data.to_entry(), reader=self.qname(), kwargs=find_funcs(self.kwargs), output_instance=self.output_instance)


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
        with fsspec.open(self.data.url, mode="rb", **(self.data.storage_options or {})) as f:
            return f.read()

    def read(self, **kwargs):
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


class PandasSQLAlchemy(BaseReader):
    implements = {datatypes.SQLQuery}
    func = "pandas:read_sql"
    imports = {"sqlalchemy", "pandas"}
    output_instance = "pandas:DataFrame"

    def discover(self, **kwargs):
        if "chunksize" not in kwargs:
            kwargs["chunksize"] = 10
        read_sql = import_name(self.func)
        return next(iter(read_sql(sql=self.data.query, con=self.data.conn, **kwargs)))

    def read(self, **kwargs):
        read_sql = import_name(self.func)
        return read_sql(sql=self.data.query, con=self.data.conn, **kwargs)


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
        if isinstance(conn, str):
            # https://duckdb.org/docs/extensions/
            if conn.startswith("sqlite:"):
                duckdb.connect(":default:").execute("INSTALL sqlite;LOAD sqlite;")
                conn = re.sub("^sqlite3?://", "", conn)
                conn = {"database": conn}
            elif conn.startswith("postgres"):
                d = duckdb.connect()
                d.execute("INSTALL postgres;LOAD postgres;")
                # extra params possible here https://duckdb.org/docs/extensions/postgres_scanner#usage
                d.execute(f"CALL postgres_attach('{conn}');")
                self._dd[str(conn)] = d
        if str(conn) not in self._dd:
            self._dd[str(conn)] = duckdb.connect(**conn)  # connection must be cached for results to be usable
        return self._dd[str(conn)]


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
        words = len(self.data.query.split())
        q = self.data.query if words > 1 else f"SELECT * FROM {self.data.query}"
        return self._duck().query(q)


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
    implements = {datatypes.TiledService}
    imports = {"tiled"}
    # a Node can convert to a Catalog
    output_instance = "tiled.client.node:Node"
    func = "tiled.client:from_uri"

    def read(self, **kwargs):
        opts = self.data.options.copy()
        opts.update(self.kwargs)
        opts.update(kwargs)
        return self._func(self.data.url, **opts)


class TiledClient(BaseReader):
    # returns dask/normal x/array/dataframe
    implements = {datatypes.TiledDataset}
    output_instance = "tiled.client.base:BaseClient"

    def read(self, as_client=True, dask=False, **kwargs):
        from tiled.client import from_uri

        opts = self.data.options.copy()
        opts.update(self.kwargs)
        opts.update(kwargs)
        if dask:
            opts["structure_clients"] = "dask"
        client = from_uri(self.data.url, **opts)
        if as_client:
            return client
        else:
            return client.read()


class PythonModule(BaseReader):
    output_instance = "builtins:module"
    implements = {datatypes.PythonSourceCode}

    def read(self, module_name=None, **kwargs):
        from types import ModuleType

        if module_name is None:
            module_name = self.data.url.rsplit("/", 1)[-1].split(".", 1)[0]
        with fsspec.open(self.data.url, "rt", **(self.data.storage_options or {})) as f:
            mod = ModuleType(module_name)
            exec(f.read(), mod.__dict__)
            return mod


def recommend(data):
    """Show which readers claim to support the given data instance"""
    out = {"importable": set(), "not_importable": set()}
    for cls in subclasses(BaseReader):
        if any(type(data) == imp for imp in cls.implements):
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
