"""Classes for reading data into a python objects"""

from __future__ import annotations

import inspect
import itertools
import json
import os
import re
from functools import lru_cache

import fsspec
from fsspec.callbacks import _DEFAULT_CALLBACK as DEFAULT_CALLBACK

import intake.readers.datatypes
from intake import import_name, logger
from intake.readers import datatypes
from intake.readers.mixins import PipelineMixin
from intake.readers.utils import Tokenizable, subclasses, port_in_use, find_free_port
from intake.utils import is_fsspec_url


class BaseReader(Tokenizable, PipelineMixin):
    imports: set[str] = set()  #: top-level packages required to use this
    implements: set[datatypes.BaseData] = set()  #: datatype(s) this applies to
    optional_imports: set[str] = set()  #: packages that might be required by some options
    func: str = "builtins:NotImplementedError"  #: function name for loading data
    func_doc: str = None  #: docstring origin if not from func
    output_instance: str = None  #: type the reader produces
    other_funcs: set[str] = set()  #: function names to recognise when matching user calls

    def __init__(
        self,
        *args,
        metadata: dict | None = None,
        output_instance: str | None = None,
        **kwargs,
    ):
        if (
            self.implements
            and (args and not isinstance(args[0], datatypes.BaseData))
            and not any(isinstance(_, datatypes.BaseData) for _ in kwargs.values())
        ):
            # reader requires data input, but not given - guess
            if len(self.implements) == 1:
                d_cls = list(self.implements)[0]
                sig = inspect.signature(d_cls.__init__).parameters
                kw2 = {}
                for k, v in kwargs.copy().items():
                    if k in sig:
                        kw2[k] = kwargs.pop(k)
                args = (d_cls(*args, **kw2),)
            else:
                raise NotImplementedError(
                    "Guessing the data type not supported for a reader implementing multiple data "
                    "classes. Please Instantiate the data type directly."
                )

        self.kwargs = kwargs
        if args:
            self.kwargs["args"] = args
        met = {}
        for a in itertools.chain(
            reversed(kwargs.get("args", [])), reversed(kwargs.values()), reversed(args)
        ):
            if isinstance(a, datatypes.BaseData):
                met.update(a.metadata)
        met.update(metadata or {})
        self.metadata = met
        if output_instance:
            self.output_instance = output_instance

    def __repr__(self):
        return f"{type(self).__name__} reader producing {self.output_instance}"

    def __call__(self, *args, **kwargs):
        """New version of this instance with altered arguments"""
        kw = self.kwargs.copy()
        kw.update(kwargs)
        if args:
            kw["args"] = args
        return type(self)(**kw)

    @classmethod
    def doc(cls):
        """Doc associated with loading function"""
        f = cls.func_doc or cls.func
        if isinstance(f, str):
            f = import_name(f)
        upstream = f.__doc__ if f is not NotImplementedError else ""
        sig = str(inspect.signature(cls._read))
        doc = cls._read.__doc__
        return "\n\n".join(_ for _ in [cls.qname(), cls.__doc__, sig, doc, upstream] if _)

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

    def read(self, *args, **kwargs):
        """Produce data artefact

        Any of the arguments encoded in the data instance can be overridden.

        Output type is given by the .output_instance attribute
        """
        logger.debug("Reading %s", self)
        kw = self.kwargs.copy()
        kw.update(kwargs)
        args = kw.pop("args", ()) or args
        return self._read(*args, **kw)

    def _read(self, *args, **kwargs):
        """This is the method subclasses will tend to override"""
        raise NotImplementedError

    def to_entry(self):
        """Create an entry version of this, ready to be inserted into a Catalog"""
        from intake.readers.entry import ReaderDescription

        return ReaderDescription(
            reader=self.qname(),
            kwargs=self.kwargs,
            output_instance=self.output_instance,
            metadata=self.metadata,
        )

    def to_cat(self, name=None):
        """Create a Catalog containing on this reader"""
        return self.to_entry().to_cat(name)

    @property
    def data(self):
        """The BaseData this reader depends on, if it has one"""
        data = self.kwargs.get("data")
        if data is None:
            args = self.kwargs.get("args", ())
            if not (args):
                raise ValueError("Cloud not find a data entity in this reader")
            data = args[0]
        if not isinstance(data, datatypes.BaseData):
            raise ValueError("Data argument isn't a BaseData")
        return data

    def to_reader(self, outtype: tuple[str] | str | None = None, reader: str | None = None, **kw):
        """Make a different reader for the data used by this reader"""
        return self.data.to_reader(outtype=outtype, reader=reader, metadata=self.metadata, **kw)

    def auto_pipeline(self, outtype: str | tuple[str], avoid: list[str] | None = None):
        from intake import auto_pipeline

        return auto_pipeline(self, outtype=outtype, avoid=avoid)

    @classmethod
    def is_ok(cls, data) -> bool:
        """Determine whether this reader is suitable for the given data instance.

        This is called after the type-based ``implements`` check and allows a
        reader to inspect the *properties* of a concrete data instance (e.g.
        the shape of its URL, whether it is a remote resource, etc.) to decide
        whether it should be recommended.

        Override this in subclasses to add instance-level constraints on top of
        the class-level ``implements`` declaration.

        Parameters
        ----------
        data:
            The ``BaseData`` instance being evaluated.

        Returns
        -------
        bool
            ``True`` if this reader can handle the data instance, ``False`` to
            exclude it from the ``recommend()`` results.
        """
        return True


class FileReader(BaseReader):
    """Convenience superclass for readers of files"""

    url_arg = "url"
    other_urls = {}  # if we have other_funcs, may have different url_args for each
    storage_options = False

    def _read(self, data, **kw):
        kw[self.url_arg] = data.url
        if self.storage_options and data.storage_options:
            kw["storage_options"] = data.storage_options
        return self._func(**kw)

    @classmethod
    def _url_is_single(cls, data) -> bool:
        """Return True when the data URL is a single plain string path/URL.

        Readers whose underlying function only accepts one file at a time
        should use this as their ``is_ok`` body.  A URL is considered
        *multi-file* when it is a list/tuple/set, or a string containing a
        glob wildcard (``*``, ``?``, ``[…]``) or a named placeholder
        (``{name}``).
        """
        url = getattr(data, "url", None)
        if isinstance(url, (list, tuple, set)):
            return False
        if isinstance(url, str):
            return not any(c in url for c in ("*", "?", "{"))
        return False

    @classmethod
    def _url_is_multi(cls, data) -> bool:
        """Return True when the data URL represents more than one file.

        Readers whose underlying function natively handles globs/lists
        (e.g. Dask, Polars scan_*, Ray) but should *not* be recommended for
        a bare single path can use this as their ``is_ok`` body.  It is the
        logical complement of ``_url_is_single``.
        """
        return not cls._url_is_single(data)


class PanelImageViewer(FileReader):
    output_instance = "panel.pane:Image"
    implements = {datatypes.PNG, datatypes.JPEG}
    func = "panel.pane:Image"
    url_arg = "object"

    @classmethod
    def is_ok(cls, data) -> bool:
        """panel.pane.Image accepts a single file path or URL only."""
        return cls._url_is_single(data)


class Pandas(FileReader):
    imports = {"pandas"}
    output_instance = "pandas:DataFrame"
    storage_options = True

    @classmethod
    def is_ok(cls, data) -> bool:
        """pandas I/O functions accept a single file path or URL only."""
        return cls._url_is_single(data)


class PandasParquet(Pandas):
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "pandas:read_parquet"
    url_arg = "path"


class PandasFeather(Pandas):
    implements = {datatypes.ArrowIPC, datatypes.Feather1}
    imports = {"pandas", "pyarrow"}
    func = "pandas:read_feather"
    url_arg = "path"


class PandasORC(Pandas):
    implements = {datatypes.ORC}
    imports = {"pandas", "pyarrow"}
    func = "pandas:read_orc"
    url_arg = "path"


class PandasExcel(Pandas):
    implements = {datatypes.Excel}
    imports = {"pandas", "openpyxl"}
    func = "pandas:read_excel"
    url_arg = "io"


class PandasSQLAlchemy(BaseReader):
    implements = {datatypes.SQLQuery}
    func = "pandas:read_sql"
    imports = {"sqlalchemy", "pandas"}
    output_instance = "pandas:DataFrame"

    def discover(self, **kwargs):
        if "chunksize" not in kwargs:
            kwargs["chunksize"] = 10
        return next(iter(self.read(**kwargs)))

    def _read(self, data, **kwargs):
        read_sql = import_name(self.func)
        return read_sql(sql=data.query, con=data.conn, **kwargs)


class DaskDF(FileReader):
    imports = {"dask", "pandas"}
    output_instance = "dask.dataframe:DataFrame"
    storage_options = True

    def discover(self, **kwargs):
        return self.read().head()


class DaskParquet(DaskDF):
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "dask.dataframe:read_parquet"
    url_arg = "path"


class DaskGeoParquet(DaskParquet):
    imports = {"dask", "geopandas"}
    func = "dask_geopandas:read_parquet"
    output_instance = "dask_geopandas.core:GeoDataFrame"


class DaskHDF(DaskDF):
    implements = {datatypes.HDF5}
    optional_imports = {"h5py"}
    func = "dask.dataframe:read_hdf"
    url_arg = "pattern"

    def _read(self, data, **kw):
        return self._func(data.url, key=data.path, **kw)


class DaskJSON(DaskDF):
    implements = {datatypes.JSONFile}
    func = "dask.dataframe:read_json"
    url_arg = "url_path"


class DaskDeltaLake(DaskDF):
    implements = {datatypes.DeltalakeTable}
    imports = {"dask_deltatable"}
    func = "dask_deltatable:read_deltalake"
    url_arg = "path"


class DaskSQL(BaseReader):
    implements = {datatypes.SQLQuery}
    imports = {"dask", "pandas", "sqlalchemy"}
    func = "dask.dataframe:read_sql"

    def _read(self, data, index_col, **kw):
        """Dask requires `index_col` to partition the dataframe on."""
        return self._func(data.quary, data.conn, index_col, **kw)


class DaskNPYStack(FileReader):
    """Requires a directory with .npy files and an "info" pickle file"""

    # TODO: single npy file, or stack without info (which can be read from any one file)
    implements = {datatypes.NumpyFile}
    imports = {"dask", "numpy"}
    func = "dask.array:from_npy_stack"
    output_instance = "dask.array:Array"
    url_arg = "dirname"

    @classmethod
    def is_ok(cls, data) -> bool:
        """dask.array.from_npy_stack requires a single directory path, not a glob or list."""
        return cls._url_is_single(data)


class DaskZarr(FileReader):
    implements = {datatypes.Zarr}
    imports = {"dask", "zarr"}
    output_instance = "dask.array:Array"
    func = "dask.array:from_zarr"

    def _read(self, data, **kwargs):
        return self._func(
            url=data.url,
            component=data.root or None,
            storage_options=data.storage_options,
            **kwargs,
        )


class NumpyZarr(FileReader):
    implements = {datatypes.Zarr}
    imports = {"zarr"}
    output_instance = "numpy:ndarray"
    func = "zarr:open"

    def _read(self, data, **kwargs):
        return self._func(data.url, storage_options=data.storage_options, path=data.root, **kwargs)[
            :
        ]


class DuckDB(BaseReader):
    imports = {"duckdb"}
    output_instance = "duckdb:DuckDBPyRelation"  # can be converted to pandas with .df
    func_doc = "duckdb:query"
    implements = {datatypes.SQLQuery}
    _dd = {}  # hold the engines, so results are still valid

    @classmethod
    def is_ok(cls, data) -> bool:
        """DuckDB readers embed the URL directly in a SQL string literal.

        Only a single plain string path or URL is supported — lists and glob
        patterns would break the SQL interpolation.
        """
        return FileReader._url_is_single(data) if hasattr(data, "url") else True

    def discover(self, **kwargs):
        return self.read().limit(10)

    @classmethod
    def _duck(cls, data, conn=None):
        import duckdb

        conn = getattr(data, "conn", conn) or {}  # only SQL type normally has this
        if str(conn) not in cls._dd:
            # TODO:  separate engine creation and caching?
            if isinstance(conn, str):
                # https://duckdb.org/docs/extensions/
                if conn.startswith("sqlite:"):
                    duckdb.connect(":default:").execute("INSTALL sqlite;LOAD sqlite;")
                    conn1 = re.sub("^sqlite3?:/{0,3}", "", conn)
                    conn1 = {"database": conn1}
                    d = duckdb.connect(**conn1)
                elif conn.startswith("postgres"):
                    d = duckdb.connect()
                    d.execute("INSTALL postgres;LOAD postgres;")
                    # extra params possible here https://duckdb.org/docs/extensions/postgres_scanner#usage
                    d.execute(f"CALL postgres_attach('{conn}');")
                else:
                    d = duckdb.connect(conn)
            else:
                d = duckdb.connect(**conn)
            cls._dd[str(conn)] = d  # connection must be cached for results to be usable
        d = cls._dd[str(conn)]
        if isinstance(data, datatypes.FileData) and "://" in data.url:
            d.execute("INSTALL httpfs;LOAD httpfs;")
        return d


class DuckParquet(DuckDB, FileReader):
    implements = {datatypes.Parquet}

    def _read(self, data, **kwargs):
        return self._duck(data).query(f"SELECT * FROM read_parquet('{data.url}')")


class DuckCSV(DuckDB, FileReader):
    implements = {datatypes.CSV}

    def _read(self, data, **kwargs):
        return self._duck(data).query(f"SELECT * FROM read_csv_auto('{data.url}')")


class DuckJSON(DuckDB, FileReader):
    implements = {datatypes.JSONFile}

    def _read(self, data, **kwargs):
        return self._duck(data).query(f"SELECT * FROM read_json_auto('{data.url}')")


class DuckSQL(DuckDB):
    implements = {datatypes.SQLQuery}

    def _read(self, data, **kwargs):
        words = len(data.query.split())
        q = data.query if words > 1 else f"SELECT * FROM {data.query}"
        return self._duck(data).query(q)


class SparkDataFrame(FileReader):
    imports = {"pyspark"}
    func = "pyspark.sql:SparkSession.builder.getOrCreate"
    func_doc = "pyspark.sql:SparkSession.read"
    output_instance = "pyspark.sql:DataFrame"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)


class SparkCSV(SparkDataFrame):
    implements = {datatypes.CSV}

    def _read(self, data, **kwargs):
        return self._func().read.csv(data.url, **kwargs)


class SparkParquet(SparkDataFrame):
    implements = {datatypes.Parquet}

    def _read(self, data, **kwargs):
        return self._func().read.parquet(data.url, **kwargs)


class SparkText(SparkDataFrame):
    implements = {datatypes.FileData}

    def _read(self, data, **kwargs):
        return self._func().read.text(data.url, **kwargs)


class SparkDeltaLake(SparkDataFrame):
    implements = {datatypes.DeltalakeTable}
    imports = {"pyspark", "delta-spark"}

    def _read(self, data, **kw):
        # see https://docs.delta.io/latest/quick-start.html#python for config
        return self._func().read.format("delta").load(data.url, **kw)


class HuggingfaceReader(BaseReader):
    imports = {"datasets"}
    implements = {datatypes.HuggingfaceDataset}
    func = "datasets:load_dataset"
    output_instance = "datasets.arrow_dataset:Dataset"

    def _read(self, data, *args, **kwargs):
        return self._func(data.name, split=data.split, **kwargs)


class SKLearnExampleReader(BaseReader):
    func = "sklearn:datasets"
    imports = {"sklearn"}
    output_instance = "sklearn.utils:Bunch"

    def _read(self, name, **kw):
        import sklearn.datasets

        loader = getattr(sklearn.datasets, f"load_{name}", None) or getattr(
            sklearn.datasets, f"fetch_{name}"
        )
        return loader()


class LlamaServerReader(BaseReader):
    """Create llama.cpp server using local pretrained model file

    The read() method allows you to pass arguments directly to the llama.cpp server
    as kwargs. Common arguments are

    host: (str) hostname for the the server to listen on, default: 127.0.0.1
    port: (int) port number for the server to listen on, default: 0, which means first free port
    system_prompt_file: (uri) Special handling here to support fsspec uri where the file is cached to disk first

    Additional kwargs not passed to llama.cpp

    startup_timeout: (int) time in seconds to wait for server to respond to a health check before failing, default 60
    callback: fsspec.callbacks.Callback derived instance progress indicator during model download, default None

    Any remaining kwargs are passed as '--<key> <value>' to llama.cpp. Where '_' is replaced with '-'. For
    options that do not take arguments, like `--verbose` the value should  be None or the empty string "".

    The following short-name arguments are supported as kwargs, they will be transformed to long-form when
    passed to llama.cpp. If a short-name option is missing it is best to use the long-form.
    """

    output_instance = "intake.readers.datatypes:LlamaCPPService"
    implements = {datatypes.GGUF}
    imports = {"requests"}

    _short_kwargs = {
        "v": "verbose",
        "s": "seed",
        "t": "threads",
        "tb": "threads-draft",
        "tbd": "threads-batch-draft",
        "ps": "p-split",
        "lcs": "lookup-cache-static",
        "lcd": "lookup-cache-dynamic",
        "c": "ctx-size",
        "n": "predict",
        "b": "batch-size",
        "ub": "ubatch-size",
        "fa": "flash-attn",
        "p": "prompt",
        "f": "file",
        "bf": "binary-file",
        "e": "escape",
        "ptc": "prompt-token-count",
        "r": "reverse-prompt",
        "sp": "special",
        "cnv": "conversation",
        "l": "logit-bias",
        "j": "json-schema",
        "gan": "grp-attn-n",
        "gaw": "grp-attn-w",
        "dkvc": "dump-kv-cache",
        "nkvo": "no-ko-offload",
        "ctk": "cache-type-k",
        "ctv": "cache-type-v",
        "dt": "defrag-thold",
        "np": "parallel",
        "ns": "sequences",
        "cb": "cont-batching",
        "ngl": "gpu-layers",
        "ngld": "gpu-layers-draft",
        "sm": "split-mode",
        "ts": "tensor-split",
        "mg": "main-gpu",
        "md": "model-draft",
        "o": "output",
        "sps": "slot-prompt-similarity",
        "ld": "logdir",
    }

    @classmethod
    def _short_kwargs_docs(cls):
        return "\n".join(f"    -{k:4s}-> --{v}" for k, v in cls._short_kwargs.items())

    @classmethod
    @lru_cache()
    def _find_executable(cls):
        import shutil

        # executables were renamed in https://github.com/ggerganov/llama.cpp/pull/7809
        path = shutil.which("llama-server")
        if path is None:
            # fallback on old name
            path = shutil.which("server")
        return path

    @classmethod
    def check_imports(cls):
        imports = super().check_imports()
        path = cls._find_executable()
        return imports & (path is not None)

    def _local_model_path(self, data, callback=DEFAULT_CALLBACK):
        import os
        from fsspec.core import split_protocol
        from intake.catalog.default import user_data_dir

        protocol, _ = split_protocol(data.url)
        if protocol is None:
            # no protocol means local path
            return data.url

        storage_options = {} if data.storage_options is None else data.storage_options
        cache_location = os.path.join(user_data_dir(), "llama.cpp")
        options = {
            protocol: storage_options,
            "simplecache": {"cache_storage": cache_location},
        }
        fs, path = fsspec.core.url_to_fs(f"simplecache::{data.url}", **options)

        cached_fn = fs._check_file(path)
        if cached_fn:
            return cached_fn

        sha = fs._mapper(path)
        cached_fn = os.path.join(fs.storage[-1], sha)

        fs.fs.get_file(path, cached_fn, callback=callback)
        return cached_fn

    def _read(self, data, log_file="llama-cpp.log", **kwargs):
        startup_timeout = kwargs.pop("startup_timeout", 60)
        callback = kwargs.pop("callback", DEFAULT_CALLBACK)

        port = kwargs.pop("port", 0)
        host = kwargs.pop("host", "127.0.0.1")

        if port == 0:
            port = find_free_port()

        URL = f"http://{host}:{port}"
        if port_in_use(host, port):
            raise RuntimeError(f"{URL} in use.")

        import requests
        import subprocess
        import atexit

        f = open(log_file, "wb")
        server_path = self._find_executable()
        path = self._local_model_path(data, callback=callback)
        cmd = [server_path, "-m", path, "--host", host, "--port", str(port), "--log-disable"]
        for k, v in kwargs.items():
            if k in self._short_kwargs:
                k = self._short_kwargs[k]

            k = k.replace("_", "-")
            if not k.startswith("-"):
                k = f"--{k}"

            if k == "--system-prompt-file":
                path = fsspec.open_local(f"simplecache::{v}")
                cmd.extend([str(k), path])
            elif v not in [None, ""]:
                cmd.extend([str(k), str(v)])
            else:
                cmd.append(str(k))

        P = subprocess.Popen(cmd, stdout=f, stderr=f)
        import time

        t0 = time.time()
        while True:
            try:
                res = requests.get(f"{URL}/health")
                if res.ok:
                    break
            except requests.ConnectionError:
                pass
            elapsed = time.time() - t0
            if (P.poll() is not None) or (elapsed > startup_timeout):
                raise RuntimeError(
                    f"Could not start {server_path}. See {log_file} for more details."
                )

        atexit.register(P.terminate)
        return intake.readers.datatypes.LlamaCPPService(
            url=URL, options={"Process": P, "log_file": log_file}
        )


LlamaServerReader.__doc__ += f"\n{LlamaServerReader._short_kwargs_docs()}"


class LlamaCPPCompletion(BaseReader):
    implements = {datatypes.LlamaCPPService}
    imports = {"requests"}
    output_instance = "builtins:str"

    def _read(self, data, prompt: str = "", *args, **kwargs):
        import requests

        r = requests.post(
            f"{data.url}/completion",
            json={"prompt": prompt, **kwargs},
            headers={"Content-Type": "application/json"},
        )
        return r.json()["content"]


class LlamaCPPEmbedding(BaseReader):
    implements = {datatypes.LlamaCPPService}
    imports = {"requests"}
    output_instance = "builtins:str"

    def _read(self, data, prompt: str = "", *args, **kwargs):
        import requests

        r = requests.post(
            f"{data.url}/embedding",
            json={"content": prompt, **kwargs},
            headers={"Content-Type": "application/json"},
        )
        return r.json()["embedding"]


class OpenAIReader(BaseReader):
    implements = {datatypes.OpenAIService}
    imports = {"openai"}
    output_instance = "openai:OpenAI"

    def _read(self, data, **kwargs):
        import openai

        client = openai.Client(api_key=data.key, base_url=data.url, **kwargs)
        return client


class OpenAICompletion(BaseReader):
    implements = {datatypes.OpenAIService}
    imports = {"requests"}
    output_instance = "builtins:dict"
    # related high-volume endpoints, assistant, embeddings

    def _read(self, data, messages: list[dict], *args, model="gtp-3.5-turbo", **kwargs):
        import requests

        url = f"{data.url}/v1/chat/completions"
        options = data.options.copy()
        options.update(kwargs)
        r = requests.get(
            url,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {data.key}"},
            json=dict(messages=messages, **options),
        )
        return r.json()["choices"][0]["message"]


class TorchDataset(BaseReader):
    output_instance = "torch.utils.data:Dataset"

    def _read(self, modname, funcname, rootdir, **kw):
        import importlib

        mod = importlib.import_module(f"torch{modname}")
        func = getattr(mod.datasets, funcname)
        try:
            return func(rootdir, download=True)
        except TypeError:
            return func(rootdir)


class TFPublicDataset(BaseReader):
    # contains ({split: tensorflow.data.Dataset}, data_info) by default
    output_instance = "builtins:tuple"
    func = "tensorflow_datasets:load"

    def _read(self, name, *args, **kwargs):
        return self._func(name, download=True, with_info=True, **kwargs)


class TFORC(FileReader):
    imports = {"tensorflow_io"}
    implements = {datatypes.ORC}
    func = "tensorflow_io:IODataset.from_orc"
    url_arg = "filename"
    output_instance = "tensorflow.data:Dataset"


class TFSQL(BaseReader):
    imports = {"tensorflow_io"}
    implements = {datatypes.SQLQuery}
    func = "tensorflow_io:experimental.IODataset.from_sql"
    output_instance = "tensorflow.data:Dataset"

    def _read(self, data, **kwargs):
        return self._func(endpoint=data.conn, query=data.query, **kwargs)


class KerasImageReader(FileReader):
    imports = {"keras"}
    implements = {datatypes.PNG, datatypes.JPEG}  # others
    func = "keras.utils:image_dataset_from_directory"
    output_instance = "tensorflow.data:Dataset"
    url_arg = "directory"

    @classmethod
    def is_ok(cls, data) -> bool:
        """keras.utils.image_dataset_from_directory requires a single directory path."""
        return cls._url_is_single(data)


class KerasAudio(FileReader):
    imports = {"keras"}
    implements = {datatypes.WAV}
    func = "keras.utils:audio_dataset_from_directory"
    output_instance = "tensorflow.data:Dataset"
    url_arg = "directory"


class KerasModelReader(FileReader):
    imports = {"keras"}
    implements = {datatypes.KerasModel}
    func = "tensorflow.keras.models:load_model"
    url_arg = "filepath"
    output_instance = "keras.engine.training:Model"


class TFRecordReader(FileReader):
    imports = {"tensorflow"}
    implements = {datatypes.TFRecord}
    func = "tensorflow.data:TFRecordDataset"
    output_instance = "tensorflow.data:TFRecordDataset"
    url_arg = "filenames"


class SKLearnModelReader(FileReader):
    # https://scikit-learn.org/stable/model_persistence.html
    # recommends skops, which seems little used
    imports = {"sklearn"}
    implements = {datatypes.SKLearnPickleModel}
    func = "pickle:load"
    output_instance = "sklearn.base:BaseEstimator"

    def _read(self, data, **kw):
        with fsspec.open(data.url, **(data.storage_options or {})) as f:
            return self._func(f)


class Awkward(FileReader):
    imports = {"awkward"}
    output_instance = "awkward:Array"
    storage_options = True


class AwkwardParquet(Awkward):
    implements = {datatypes.Parquet}
    imports = {"awkward", "pyarrow"}
    func = "awkward:from_parquet"
    url_arg = "path"

    def discover(self, **kwargs):
        kwargs["row_groups"] = [0]
        return self.read(**kwargs)


class DaskAwkwardParquet(AwkwardParquet):
    imports = {"dask_awkward", "pyarrow", "dask"}
    func = "dask_awkward:from_parquet"
    output_instance = "dask_awkward:Array"

    def discover(self, **kwargs):
        return self.read(**kwargs).partitions[0]


class AwkwardJSON(Awkward):
    implements = {datatypes.JSONFile}
    func = "awkward:from_json"
    url_arg = "source"


class AwkwardAVRO(Awkward):
    implements = {datatypes.AVRO}
    func = "awkward:from_avro_file"
    url_arg = "file"

    @classmethod
    def is_ok(cls, data) -> bool:
        """awkward.from_avro_file accepts a single file path or file-like object only."""
        return cls._url_is_single(data)


class DaskAwkwardJSON(Awkward):
    imports = {"dask_awkward", "dask"}
    func = "dask_awkward:from_json"
    output_instance = "dask_awkward:Array"
    url_arg = "source"

    def discover(self, **kwargs):
        return self.read(**kwargs).partitions[0]


class HandleToUrlReader(BaseReader):
    """Dereference handle (hdl:) identifiers

    See handle.net for a description of the registry.
    """

    implements = {datatypes.Handle}
    func = "requests:get"
    imports = {"requests", "aiohttp"}
    output_instance = datatypes.BaseData.qname()

    @classmethod
    def _extract(cls, meta, base):
        h = fsspec.filesystem("http")
        if "URL_ORIGINAL_DATA" in meta:
            # file
            url = re.findall('href="(.*?)"', meta["URL_ORIGINAL_DATA"]["value"])[0]
        elif "HAS_PARTS" in meta:
            # dataset
            ids = meta["HAS_PARTS"]["value"].split(";")
            rr = h.cat([f"{base}/{u.lstrip('hdl:/')}" for u in ids])
            rr2 = [{i["type"]: i["data"] for i in json.loads(r)["values"]} for r in rr.values()]
            url = [cls._extract(r2, base) for r2 in rr2]
        return url

    def _read(self, data, base="https://hdl.handle.net/api/handles", **kwargs):
        h = fsspec.filesystem("http")
        r = h.cat(f"{base}/{data.url.lstrip('hdl:/')}")
        j = json.loads(r)
        meta = {i["type"]: i["data"] for i in j["values"]}
        url = self._extract(meta, base)
        # TODO: we can assume HDF->xarray here?
        cls = datatypes.recommend(url[0] if isinstance(url, list) else url)[0]
        return cls(url=url, metadata=meta)


class PandasCSV(Pandas):
    implements = {datatypes.CSV}
    func = "pandas:read_csv"
    url_arg = "filepath_or_buffer"

    def discover(self, **kw):
        kw["nrows"] = 10
        kw.pop("skipfooter", None)
        kw.pop("chunksize", None)

        return self.read(**kw)


class PandasHDF5(Pandas):
    implements = {datatypes.HDF5}
    func = "pandas:read_hdf"
    imports = {"pandas", "pytables"}

    @classmethod
    def is_ok(cls, data) -> bool:
        """pandas.read_hdf only supports local file paths — no remote URLs or globs."""
        url = getattr(data, "url", None)
        if not isinstance(url, str):
            return False
        return "://" not in url and "::" not in url and not any(c in url for c in ("*", "?", "{"))

    def _read(self, data, **kw):
        if data.storage_options:  # or fsspec-like
            with fsspec.open(data.url, "rb", **data.storage_options) as f:
                self._func(f, data.path, **kw)
        return self._func(data.url, **kw)


class DaskCSV(DaskDF):
    implements = {datatypes.CSV}
    func = "dask.dataframe:read_csv"
    url_arg = "urlpath"


class DaskText(FileReader):
    imports = {"dask"}
    implements = {datatypes.FileData}
    func = "dask.bag:read_text"
    output_instance = "dask.bag.core:Bag"
    storage_options = True
    url_arg = "urlpath"

    def discover(self, n=10, **kwargs):
        return self.read().take(n)


class DaskCSVPattern(DaskCSV):
    """Apply categorical data extraction to a set of CSV paths using dask

    Paths are of the form "proto://path/{field}/measurement_{date:%Y-%m-%d}.csv",
    where the format-like fields will be captured as columns in the output.
    """

    implements = {datatypes.CSVPattern}

    def _read(self, data, **kw):
        from pandas.api.types import CategoricalDtype

        from intake.readers.utils import pattern_to_glob
        from intake.source.utils import reverse_formats

        url = pattern_to_glob(data.url)
        df = self._func(url, storage_options=data.storage_options, include_path_column=True, **kw)

        paths = sorted(df["path"].cat.categories)

        column_by_field = {
            field: df["path"]
            .cat.codes.map(dict(enumerate(values)))
            .astype(CategoricalDtype(set(values)))
            for field, values in reverse_formats(data.url, paths).items()
        }

        return df.assign(**column_by_field).drop(columns=["path"])


class Polars(FileReader):
    imports = {"polars"}
    output_instance = "polars:LazyFrame"
    url_arg = "source"

    def discover(self, **kwargs):
        # https://pola-rs.github.io/polars/py-polars/html/reference/
        #   lazyframe/api/polars.LazyFrame.fetch.html
        return self.read().fetch()


class PolarsDeltaLake(Polars):
    implements = {datatypes.DeltalakeTable}
    func = "polars:scan_delta"


class PolarsAvro(Polars):
    implements = {datatypes.AVRO}
    func = "polars:read_avro"
    output_instance = "polars:DataFrame"  # i.e., not lazy

    @classmethod
    def is_ok(cls, data) -> bool:
        """polars.read_avro accepts a single file path or file-like object only."""
        return cls._url_is_single(data)


class PolarsParquet(Polars):
    implements = {datatypes.Parquet}
    func = "polars:scan_parquet"


class PolarsCSV(Polars):
    implements = {datatypes.CSV}
    func = "polars:scan_csv"


class PolarsJSON(Polars):
    implements = {datatypes.JSONFile}
    func = "polars:scan_ndjson"


class PolarsIceberg(Polars):
    imports = {"polars", "pyiceberg"}
    implements = {datatypes.IcebergDataset}
    func = "polars:scan_iceberg"


class PolarsExcel(Polars):
    implements = {datatypes.Excel}
    func = "polars:read_excel"
    output_instance = "polars:DataFrame"  # i.e., not lazy

    @classmethod
    def is_ok(cls, data) -> bool:
        """polars.read_excel accepts a single file path or file-like object only."""
        return cls._url_is_single(data)


class Ray(FileReader):
    # https://docs.ray.io/en/latest/data/creating-datasets.html#supported-file-formats
    imports = {"ray"}
    output_instance = "ray.data:Dataset"
    url_arg = "paths"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)

    def _read(self, data, **kw):
        if (
            data.url.startswith("s3://")
            and data.storage_options
            and data.storage_options.get("anon")
        ):
            data = type(data)(url=f"s3://anonymous@{data.url[5:]}")
        # TODO: other auth parameters, key/secret, token
        #  apparently, creating an S3FileSystem here is also allowed
        return super()._read(data, **kw)


class RayParquet(Ray):
    implements = {datatypes.Parquet}
    func = "ray.data:read_parquet"


class RayCSV(Ray):
    implements = {datatypes.CSV}
    func = "ray.data:read_csv"


class RayJSON(Ray):
    implements = {datatypes.JSONFile}
    func = "ray.data:read_json"


class RayBinary(Ray):
    implements = {datatypes.FileData}
    func = "ray.data:read_binary_files"


class RayDeltaLake(Ray):
    implements = {datatypes.DeltalakeTable}
    imports = {"deltaray"}
    func = "deltaray:read_delta"
    url_arg = "table_uri"


class DeltaReader(FileReader):
    implements = {datatypes.Parquet, datatypes.DeltalakeTable}
    imports = {"deltalake"}
    func = "deltalake:DeltaTable"
    url_arg = "table_uri"
    storage_options = True
    output_instance = "deltalake:DeltaTable"

    @classmethod
    def is_ok(cls, data) -> bool:
        """deltalake.DeltaTable accepts a single table URI (a directory path) only."""
        return cls._url_is_single(data)


class TiledNode(BaseReader):
    implements = {datatypes.TiledService}
    imports = {"tiled"}
    output_instance = "tiled.client.node:Node"
    func = "tiled.client:from_uri"

    def _read(self, data, **kwargs):
        opts = data.options.copy()
        opts.update(kwargs)
        return self._func(data.url, **opts)


class TiledClient(BaseReader):
    # returns dask/normal x/array/dataframe
    implements = {datatypes.TiledDataset}
    output_instance = "tiled.client.base:BaseClient"

    def _read(self, data, as_client=True, dask=False, **kwargs):
        from tiled.client import from_uri

        opts = data.options.copy()
        opts.update(kwargs)
        if dask:
            opts["structure_clients"] = "dask"
        client = from_uri(data.url, **opts)
        if as_client:
            return client
        else:
            return client.read()


class TileDBReader(BaseReader):
    imports = {"tiledb"}
    implements = {datatypes.TileDB}
    output_instance = "tiledb.libtiledb.Array"
    func = "tiledb:open"

    def _read(self, data, attribute=None, **kwargs):
        return self._func(data.url, attr=attribute, config=data.options, **kwargs)


class TileDBDaskReader(BaseReader):
    imports = {"tiledb", "dask"}
    func = "dask.array:from_tiledb"
    implements = {datatypes.TileDB}
    output_instance = "dask.array:Array"

    def _read(self, data, attribute=None, **kwargs):
        return self._func(data.url, attribute=attribute, config=data.options, **kwargs)


class PythonModule(BaseReader):
    output_instance = "builtins:module"
    implements = {datatypes.PythonSourceCode}

    def _read(self, data, module_name=None, **kwargs):
        from types import ModuleType

        if module_name is None:
            module_name = data.url.rsplit("/", 1)[-1].split(".", 1)[0]
        with fsspec.open(data.url, "rt", **(data.storage_options or {})) as f:
            mod = ModuleType(module_name)
            exec(f.read(), mod.__dict__)
            return mod


class SKImageReader(FileReader):
    output_instance = "numpy:ndarray"
    imports = {"scikit-image"}
    implements = {datatypes.PNG, datatypes.TIFF, datatypes.JPEG}
    func = "skimage.io:imread"
    url_arg = "fname"

    @classmethod
    def is_ok(cls, data) -> bool:
        """skimage.io.imread reads a single image file at a time."""
        return cls._url_is_single(data)


class NumpyText(FileReader):
    output_instance = "numpy:ndarray"
    implements = {datatypes.FileData}
    imports = {"numpy"}
    func = "numpy:loadtxt"

    @classmethod
    def is_ok(cls, data) -> bool:
        """numpy.loadtxt / numpy.load accept a single file path or file-like object only."""
        return cls._url_is_single(data)

    def _read(self, data, **kw):
        if data.storage_options or "://" in data.url or "::" in data.url:
            with fsspec.open(data.url, **(data.storage_options or {})) as f:
                return self._func(f, **kw)
        return self._func(data.url, **kw)


class NumpyReader(NumpyText):
    func = "numpy:load"
    implements = {datatypes.NumpyFile}


class CupyNumpyReader(NumpyText):
    output_instance = "cupy:ndarray"
    implements = {datatypes.NumpyFile}
    imports = {"cupy"}
    func = "cupy:loadtxt"


class CupyTextReader(CupyNumpyReader):
    implements = {datatypes.FileData}
    func = "numpy:loadtxt"


class XArrayDatasetReader(FileReader):
    output_instance = "xarray:Dataset"
    imports = {"xarray"}
    optional_imports = {"zarr", "h5netcdf", "cfgrib", "scipy", "tiledb"}  # and others
    # DAP is not a file but an API, maybe should be separate
    implements = {
        datatypes.NetCDF3,
        datatypes.HDF5,
        datatypes.GRIB2,
        datatypes.IcechunkRepo,
        datatypes.Zarr,
        datatypes.OpenDAP,
        datatypes.TileDB,
    }
    # xarray also reads from images and tabular data
    func = "xarray:open_mfdataset"
    other_funcs = {"xarray:open_dataset"}
    other_urls = {"xarray:open_dataset": "filename_or_obj"}
    url_arg = "paths"

    def _read(self, data, open_local=False, **kw):
        from xarray import open_dataset, open_mfdataset

        if "engine" not in kw:
            if isinstance(data, (datatypes.Zarr, datatypes.IcechunkRepo)):
                kw["engine"] = "zarr"
                if data.root and "group" not in kw:
                    kw["group"] = data.root
            elif isinstance(data, datatypes.TileDB):
                kw["engine"] = "tiledb"
                if data.options:
                    kw.setdefault("backend_kwargs", {})["config"] = data.options
            elif isinstance(data, datatypes.NetCDF3):
                kw["engine"] = "scipy"
            elif isinstance(data, datatypes.HDF5):
                kw["engine"] = "h5netcdf"
        if kw.get("engine", "") in ["zarr", "kerchunk"] and data.storage_options:
            kw.setdefault("backend_kwargs", {})["storage_options"] = data.storage_options
        if isinstance(data, (datatypes.HDF5, datatypes.NetCDF3)):
            kw.setdefault("engine", "h5netcdf")
            if getattr(data, "path", False):
                kw["group"] = data.path
        if isinstance(data, datatypes.IcechunkRepo):
            import icechunk

            url = f"{data.url}_storage" if "storage" not in data.url else data.url
            store_cls = getattr(icechunk, url)
            store = store_cls(**(data.storage_options or {}))
            repo = icechunk.Repository.open(store)
            session = repo.readonly_session(data.ref)
            zarr_store = session.store
            kw.get("backend_kwargs", {}).pop("storage_options", None)
            kw.setdefault("backend_kwargs", {})["consolidated"] = False
            return open_dataset(zarr_store, **kw)
        auth = kw.pop("auth", "")
        if isinstance(data, datatypes.OpenDAP) and auth and kw.get("engine", "") == "pydap":
            import requests

            if isinstance(auth, str):
                setup = intake.import_name(f"pydap.cas.{auth}:setup_session")
                un = kw.pop("username", os.getenv("DAP_USER"))
                pw = kw.pop("password", os.getenv("DAP_PASSWORD"))
                session = setup(un, pw, check_url=data.url)
            else:
                session = requests.Session()
                session.auth = auth
            return open_dataset(data.url, session=session, **kw)
        if isinstance(data.url, (tuple, set, list)) and len(data.url) == 1:
            return open_dataset(data.url[0], **kw)
        elif (isinstance(data.url, (tuple, set, list)) and len(data.url) > 1) or (
            isinstance(data.url, str) and "*" in data.url
        ):
            if isinstance(data, (datatypes.Zarr, datatypes.OpenDAP)):
                ofs = data.url
            elif open_local:
                ofs = fsspec.open_local(data.url, **(data.storage_options or {}))
            elif (isinstance(data.url, str) and is_fsspec_url(data.url)) or is_fsspec_url(
                data.url[0]
            ):
                ofs0 = fsspec.open_files(data.url, **(data.storage_options or {}))
                ofs = [_.open() for _ in ofs0]
                self.stash = ofs, ofs0
            else:
                ofs = data.url
            return open_mfdataset(ofs, **kw)
        else:
            if (
                isinstance(data, datatypes.FileData)
                and is_fsspec_url(data.url)
                and not isinstance(data, datatypes.Zarr)
            ):
                # special case, because xarray would assume a DAP endpoint
                if open_local:
                    f = fsspec.open_local(data.url, **(data.storage_options or {}))
                    return open_dataset(f, **kw)
                else:
                    f = fsspec.open(data.url, **(data.storage_options or {})).open()
                    return open_dataset(f, **kw)
            return open_dataset(data.url, **kw)


class XArrayPatternReader(XArrayDatasetReader):
    """Same as XarrayDatasetReader, but recognises file patterns

    If you use a URL like "/path/file_{value}_.nc". The template may include
    specifiers like ":d" to determine the type of the values inferred. This
    reader supports all the same filetypes as XArrayDatasetReader.

    The read step may be accelerated by providing arguments like
    ``parallel=True`` and ``combine_attrs="override" - see the
    xr.open_mfdataset documentation.

    Note: this method determined the ``concat_dim`` and ``combine`` arguments,
    so passing these will raise an exception.
    """

    # should we have an explicit pattern type data input?

    @classmethod
    def is_ok(cls, data) -> bool:
        """Only recommend this reader when the URL is a list/tuple of paths or a glob string.

        A single plain filename without wildcards cannot be a multi-file pattern,
        so ``XArrayPatternReader`` would have nothing useful to do with it.
        ``XArrayDatasetReader`` (the parent) handles those cases instead.
        """
        url = getattr(data, "url", None)
        if isinstance(url, (list, tuple, set)):
            return len(url) > 0
        if isinstance(url, str):
            return "*" in url or "{" in url
        return False

    def _read(self, data, open_local=False, pattern=None, **kw):
        import pandas as pd
        from intake.readers.utils import pattern_to_glob
        from intake.source.utils import reverse_formats

        if isinstance(data.url, str):
            url = pattern_to_glob(data.url)
            fs, _, paths = fsspec.get_fs_token_paths(url, **(data.storage_options or {}))
            val_dict = reverse_formats(data.url, paths)
        else:
            paths = data.url
            val_dict = reverse_formats(pattern, data.url)
        indices = [pd.Index(v, name=k) for k, v in val_dict.items()]
        data2 = type(data)(url=paths, storage_options=data.storage_options, metadata=data.metadata)
        if "concat_dim" in kw:
            ccm = kw.pop("concat_dim")
            ccm = [ccm] if isinstance(ccm, str) else ccm
            for ind, cd in zip(indices, ccm):
                ind.name = cd
        kw.setdefault("combine", "nested")
        return super()._read(data2, concat_dim=indices, open_local=open_local, **kw)


class RasterIOXarrayReader(FileReader):
    output_instance = "xarray:Dataset"
    imports = {"rioxarray"}
    implements = {datatypes.TIFF, datatypes.GDALRasterFile}
    func = "rioxarray:open_rasterio"
    url_arg = "filename"

    def _read(self, data, concat_kwargs=None, **kwargs):
        import xarray as xr
        from rioxarray import open_rasterio

        concat_kwargs = concat_kwargs or {
            k: kwargs.pop(k)
            for k in {"dim", "data_vars", "coords", "compat", "position", "join"}
            if k in kwargs
        }

        ofs = fsspec.open_files(data.url, **(data.storage_options or {}))
        opened = [open_rasterio(of.open(), **kwargs) for of in ofs]
        self.stash = opened
        if len(ofs) == 1:
            return open_rasterio(opened[0], **kwargs)
        else:
            if "dim" in concat_kwargs:
                return xr.concat(opened, **concat_kwargs)
            elif "concat_dim" in concat_kwargs:
                return xr.combine_nested(opened, **concat_kwargs)
            else:
                try:
                    return xr.combine_by_coords(opened, **concat_kwargs)
                except ValueError:
                    concat_dim = [[_.path.rsplit("/", 1)[-1].rsplit(".", 1)[0] for _ in ofs]]
                    return xr.combine_nested(opened, concat_dim=concat_dim, **concat_kwargs)


class GeoPandasReader(FileReader):
    # TODO: geopandas also supports postGIS
    output_instance = "geopandas:GeoDataFrame"
    imports = {"geopandas"}
    implements = {
        datatypes.GeoJSON,
        datatypes.CSV,
        datatypes.SQLite,
        datatypes.Shapefile,
        datatypes.GDALVectorFile,
        datatypes.GeoPackage,
        datatypes.FlatGeoBuf,
    }
    func = "geopandas:read_file"
    url_arg = "filename"

    @classmethod
    def is_ok(cls, data) -> bool:
        """geopandas.read_file reads a single vector file or URL at a time."""
        return cls._url_is_single(data)

    def _read(self, data, with_fsspec=None, **kwargs):
        import geopandas

        if with_fsspec is None:
            with_fsspec = (
                ("://" in data.url and "!" not in data.url)
                or "::" in data.url
                or data.storage_options
            )
        if with_fsspec:
            with fsspec.open(data.url, **(data.storage_options or {})) as f:
                return geopandas.read_file(f, **kwargs)
        return geopandas.read_file(data.url, **kwargs)


class ScipyMatlabReader(FileReader):
    output_instance = "numpy:ndarray"
    implements = {datatypes.MatlabArray}
    imports = {"scipy"}
    func = "scipy.io:loadmat"

    def _read(self, data, **kwargs):
        return self._func(data.path, appendmat=False, **kwargs)[data.variable]


class ScipyMatrixMarketReader(FileReader):
    output_instance = "scipy.sparse:coo_matrix"  # numpy-like
    implements = {datatypes.MatrixMarket}
    imports = {"scipy"}
    func = "scipy.io:mmread"

    def _read(self, data, **kw):
        with fsspec.open(data.url, **data.storage_options) as f:
            return self._func(f)


class NibabelNiftiReader(FileReader):
    output_instance = "nibabel.spatialimages:SpatialImage"
    implements = {datatypes.Nifti}  # and other medical image types
    imports = {"nibabel"}
    func = "nibabel:load"
    url_arg = "filename"

    def _read(self, data, **kw):
        with fsspec.open(data.url, **(data.storage_options or {})) as f:
            return self._func(f, **kw)


class FITSReader(FileReader):
    output_instance = "astropy.io.fits:HDUList"
    implements = {datatypes.FITS}
    imports = {"astropy"}
    func = "astropy.io.fits:open"

    def _read(self, data, **kw):
        if data.storage_options:
            kw.pop("use_fsspec")
            kw.pop("fsspec_kwargs")
            return self._func(data.url, use_fsspec=True, fsspec_kwargs=data.storage_options, **kw)
        return self._func(data.url, **kw)


class ASDFReader(FileReader):
    implements = {datatypes.ASDF}
    imports = {"asdf"}
    func = "asdf:open"
    output_instance = "asdf:AsdfFile"

    def _read(self, data, **kw):
        if data.storage_options or "://" in data.url or "::" in data.url:
            # want the file to stay open, since array access is lazy by default
            f = fsspec.open(data.url, **(data.storage_options or {})).open()
            return self._func(f, **kw)
        return self._func(data.url, **kw)


class DicomReader(FileReader):
    output_instance = "pydicom.dataset:FileDataset"
    implements = {datatypes.DICOM}
    imports = {"pydicom"}
    func = "pydicom:read_file"
    url_arg = "fp"  # can be file-like
    storage_options = True

    def _read(self, data, **kw):
        with fsspec.open(data.url, **(data.storage_options or {})) as f:
            return self._func(f, **kw)


class Condition(BaseReader):
    def _read(
        self, if_true, if_false, condition: callable[[BaseReader, ...], bool] | bool, **kwargs
    ):
        if isinstance(condition, bool):
            cond = condition
        elif isinstance(condition, BaseReader):
            cond = condition.read()
        else:
            cond = condition(**kwargs)
        if cond:
            return if_true.read() if isinstance(if_true, BaseReader) else if_true
        else:
            return if_false.read() if isinstance(if_false, BaseReader) else if_false


class PMTileReader(BaseReader):
    implements = {datatypes.PMTiles}
    func = "pmtiles.reader:Reader"
    output_instance = "pmtiles.reader:Reader"

    def _read(self, data):
        import pmtiles.reader

        if "://" in data.url or "::" in data.url:
            f = fsspec.open(data.url, **(data.storage_options or {})).open()

            def get_bytes(offset, length):
                f.seek(offset)
                return f.read(length)

        else:
            f = open(data.url)
            get_bytes = pmtiles.reader.MmapSource(f)
        return self._func(get_bytes)


class YAMLCatalogReader(FileReader):
    implements = {datatypes.YAMLFile, datatypes.YAMLFile}
    func = "intake.readers.entry:Catalog.from_yaml_file"
    url_arg = "path"
    storage_options = True
    output_instance = "intake.readers.entry:Catalog"


class PrometheusMetricReader(BaseReader):
    implements = {datatypes.Prometheus}
    imports = {"prometheus_api_client"}
    output_instance = "typing:Iterator"
    func = "prometheus_api_client:custom_query"
    other_funcs = {"prometheus_api_client:get_metric_range_data"}

    def _read(self, data: datatypes.Prometheus, *args, **kwargs):
        from prometheus_api_client import PrometheusConnect
        from prometheus_api_client.utils import parse_datetime

        prom = PrometheusConnect(url=data.url, **(data.options or {}))
        if data.query:
            # this is a catalog, should be separate reader?
            return prom.custom_query(data.query, **kwargs)
        if not data.metric:
            return prom.all_metrics(**kwargs)
        start_time = parse_datetime(data.start_time) if data.start_time else parse_datetime("1900")
        end_time = parse_datetime(data.end_time) if data.end_time else parse_datetime("now")
        return prom.get_metric_range_data(
            data.metric,
            label_config=data.labels,
            start_time=start_time,
            end_time=end_time,
            **kwargs,
        )


class Retry(BaseReader):
    """Retry (part of) a pipeline until it returns without exception

    Retries the whole of the selected pipeline; an exception will start at the beginning.
    """

    def _read(
        self,
        data,
        max_tries=10,
        allowed_exceptions=(Exception,),
        backoff0=0.1,
        backoff_factor=1.3,
        start_stage=None,
        **kw,
    ):
        """
        Parameters
        ----------
        data: intake pipeline/reader
        max_tries: number of attempts that can be made
        allowed_exceptions: tuple of Exceptions we will try again for; others will raise
        start_stage: if given, index of pipeline member stage to start from for retries (else all);
            may be negative from the most recent previous stage (-1).
        """
        import time

        if isinstance(allowed_exceptions, (list, set)):
            allowed_exceptions = tuple(allowed_exceptions)
        reader = data if isinstance(data, BaseReader) else data.reader
        if start_stage and start_stage < 0:
            start_stage = len(reader.steps) + start_stage
        if start_stage:
            data = reader.first_n_stages(start_stage).read()
        else:
            data = None
            start_stage = 0
        for j in range(max_tries):
            try:
                for i in range(start_stage, len(reader.steps)):
                    if i == 0:
                        data = reader._read_stage_n(stage=0)
                    else:
                        data = reader._read_stage_n(stage=1, data=data)
            except allowed_exceptions:
                if j == max_tries:
                    raise
                time.sleep(backoff0 * backoff_factor**i)
        return data


def recommend(data):
    """Show which readers claim to support the given data instance or a superclass

    The ordering is more specific readers first
    """
    seen = set()
    out = {"importable": [], "not_importable": []}
    data_instance = data if not isinstance(data, type) else None
    data_cls = type(data) if not isinstance(data, type) else data
    for cls in subclasses(BaseReader):
        if any(data_cls == imp for imp in cls.implements):
            if cls not in seen:
                seen.add(cls)
                # If we have a concrete instance (not just the type), let the
                # reader inspect its properties before recommending it.
                if data_instance is not None and not cls.is_ok(data_instance):
                    continue
                if cls.check_imports():
                    out["importable"].append(cls)
                else:
                    out["not_importable"].append(cls)
    return out


class PolarsFeather(Polars):
    implements = {datatypes.ArrowIPC}
    func = "polars:scan_ipc"
    url_arg = "source"


class PyArrowIPCReader(FileReader):
    """Read an Arrow IPC file or stream into a PyArrow Table.

    Supports both the "file" (random-access) and "stream" formats via
    ``pyarrow.ipc.open_file`` / ``open_stream`` heuristic.
    """

    implements = {datatypes.ArrowIPC}
    imports = {"pyarrow"}
    func = "pyarrow.ipc:open_file"
    output_instance = "pyarrow.lib:Table"
    url_arg = "source"

    def _read(self, data, **kwargs):
        import pyarrow.ipc as ipc

        try:
            reader = ipc.open_file(data.url, **kwargs)
        except Exception:
            reader = ipc.open_stream(data.url, **kwargs)
        return reader.read_all()

    def discover(self, **kwargs):
        import pyarrow.ipc as ipc

        data = self.kwargs.get("data") or (self.kwargs.get("args") or [None])[0]
        try:
            reader = ipc.open_file(data.url)
            return reader.read_all().slice(0, 10)
        except Exception:
            reader = ipc.open_stream(data.url)
            return reader.read_next_batch()


class PandasArrowIPC(Pandas):
    """Read an Arrow IPC file into a pandas DataFrame via PyArrow."""

    implements = {datatypes.ArrowIPC}
    imports = {"pandas", "pyarrow"}
    func = "pandas:read_feather"
    url_arg = "path"


class GeoPandasTabular(FileReader):
    output_instance = "geopandas:GeoDataFrame"
    imports = {"geopandas", "pyarrow"}
    implements = {datatypes.Parquet, datatypes.ArrowIPC}
    func = "geopandas:read_parquet"
    other_funcs = {"geopandas:read_feather"}
    url_arg = "path"

    @classmethod
    def is_ok(cls, data) -> bool:
        """geopandas.read_parquet / read_feather each accept a single file path."""
        return cls._url_is_single(data)

    def _read(self, data, **kwargs):
        import geopandas

        if "://" in data.url or "::" in data.url:
            f = fsspec.open(data.url, **(data.storage_options or {})).open()
        else:
            f = data.url
        if isinstance(data, datatypes.Parquet):
            return geopandas.read_parquet(f, **kwargs)
        elif isinstance(data, datatypes.ArrowIPC):
            return geopandas.read_feather(f, **kwargs)
        else:
            raise ValueError


class PolarsArrowIPC(Polars):
    """Read an Arrow IPC file lazily with Polars."""

    implements = {datatypes.ArrowIPC}
    func = "polars:scan_ipc"
    url_arg = "source"


# ---------------------------------------------------------------------------
# Lance
# ---------------------------------------------------------------------------


class LanceReader(FileReader):
    """Read a Lance dataset into a PyArrow Table (lazy scan)."""

    implements = {datatypes.Lance}
    imports = {"lancedb"}
    func = "lancedb:connect"
    output_instance = "lancedb.table:LanceTable"
    url_arg = "uri"

    def _read(self, data, table=None, **kwargs):
        import lancedb

        db = lancedb.connect(data.url)
        if table is None:
            table = db.table_names()[0]
        return db.open_table(table)

    def discover(self, **kwargs):
        tbl = self.read(**kwargs)
        return tbl.to_arrow().slice(0, 10)


class PolarsLance(FileReader):
    """Read a Lance dataset lazily with Polars (via PyArrow bridge)."""

    implements = {datatypes.Lance}
    imports = {"polars", "lancedb"}
    func = "polars:from_arrow"
    output_instance = "polars:LazyFrame"
    url_arg = "source"

    def _read(self, data, **kwargs):
        import lancedb
        import polars as pl

        db = lancedb.connect(data.url)
        tbl = db.open_table(db.table_names()[0])
        return pl.from_arrow(tbl.to_arrow()).lazy()


# ---------------------------------------------------------------------------
# MessagePack
# ---------------------------------------------------------------------------


class MessagePackReader(FileReader):
    """Read a MessagePack file into a Python dict/list."""

    implements = {datatypes.MessagePack}
    imports = {"msgpack"}
    func = "msgpack:unpack"
    output_instance = "builtins:object"
    url_arg = "file_like"

    def _read(self, data, **kwargs):
        import msgpack
        import fsspec

        with fsspec.open(data.url, "rb", **(data.storage_options or {})) as f:
            return msgpack.unpack(f, **kwargs)


# ---------------------------------------------------------------------------
# Document / text
# ---------------------------------------------------------------------------


class MarkdownReader(FileReader):
    """Read a Markdown / RST file, with a partial head for discovery.

    ``read()`` returns the full file content as a string.
    ``discover()`` reads only the first ``head_bytes`` (default 8 KB), which
    is enough to extract headings and document structure without loading
    arbitrarily large files into memory.
    """

    implements = {datatypes.Markdown, datatypes.ReStructuredText}
    imports = set()
    func = "builtins:open"
    output_instance = "builtins:str"
    url_arg = "file"

    def _read(self, data, **kwargs):
        import fsspec

        with fsspec.open(data.url, "r", **(data.storage_options or {})) as f:
            return f.read()

    def discover(self, head_bytes: int = 8192, **kwargs):
        import fsspec

        data = self.kwargs.get("data") or (self.kwargs.get("args") or [None])[0]
        with fsspec.open(data.url, "rb", **(data.storage_options or {})) as f:
            raw = f.read(head_bytes)
        # Decode leniently; replace undecodable bytes at a potential mid-char cut
        return raw.decode("utf-8", errors="replace")


class TOMLReader(FileReader):
    """Read a TOML file into a Python dict."""

    implements = {datatypes.TOML}
    imports = {"tomllib"}  # stdlib in Python 3.11+
    func = "tomllib:loads"
    output_instance = "builtins:dict"

    def _read(self, data, **kwargs):
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # backport
        import fsspec

        with fsspec.open(data.url, "rb", **(data.storage_options or {})) as f:
            return tomllib.load(f)

    @classmethod
    def check_imports(cls):
        try:
            import tomllib  # noqa: F401

            return True
        except ImportError:
            from intake.readers.utils import check_imports as _ci

            return _ci("tomli")


class TOMLReaderTomli(TOMLReader):
    """Read a TOML file using the ``tomli`` backport (Python < 3.11)."""

    imports = {"tomli"}
    func = "tomli:loads"


class INIReader(FileReader):
    """Read an INI/CFG file into a Python dict via ``configparser``."""

    implements = {datatypes.INIFile}
    imports = set()
    func = "configparser:ConfigParser"
    output_instance = "configparser:ConfigParser"

    def _read(self, data, **kwargs):
        import configparser
        import fsspec

        cfg = configparser.ConfigParser(**kwargs)
        with fsspec.open(data.url, "r", **(data.storage_options or {})) as f:
            cfg.read_file(f)
        return cfg


class HTMLTableReader(FileReader):
    """Read HTML tables from a file or URL into a list of DataFrames."""

    implements = {datatypes.HTMLFile}
    imports = {"pandas", "lxml"}
    func = "pandas:read_html"
    output_instance = "builtins:list"
    url_arg = "io"

    def discover(self, **kwargs):
        tables = self.read(**kwargs)
        return tables[0].head(10) if tables else []


class PDFTextReader(FileReader):
    """Extract text from a PDF file using ``pdfminer.six``."""

    implements = {datatypes.PDFFile}
    imports = {"pdfminer"}
    func = "pdfminer.high_level:extract_text"
    output_instance = "builtins:str"
    url_arg = "pdf_file"

    def _read(self, data, **kwargs):
        from pdfminer.high_level import extract_text
        import fsspec

        with fsspec.open(data.url, "rb", **(data.storage_options or {})) as f:
            return extract_text(f, **kwargs)


class PDFPlumberReader(FileReader):
    """Extract structured content from a PDF using ``pdfplumber``."""

    implements = {datatypes.PDFFile}
    imports = {"pdfplumber"}
    func = "pdfplumber:open"
    output_instance = "pdfplumber.pdf:PDF"
    url_arg = "path"

    def _read(self, data, **kwargs):
        import pdfplumber

        return pdfplumber.open(data.url, **kwargs)

    def discover(self, **kwargs):
        pdf = self.read(**kwargs)
        return pdf.pages[:3] if pdf.pages else pdf


class EPUBReader(FileReader):
    """Read an EPUB e-book using ``ebooklib``."""

    implements = {datatypes.EPUBFile}
    imports = {"ebooklib"}
    func = "ebooklib.epub:read_epub"
    output_instance = "ebooklib.epub:EpubBook"
    url_arg = "name"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class DOCXReader(FileReader):
    """Read a DOCX file as plain text using ``python-docx``."""

    implements = {datatypes.DOCXFile}
    imports = {"docx"}
    func = "docx:Document"
    output_instance = "docx.document:Document"
    url_arg = "docx"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


# ---------------------------------------------------------------------------
# Audio
# ---------------------------------------------------------------------------


class LibrosaAudioReader(FileReader):
    """Load an audio file as a NumPy array with sample rate using librosa."""

    implements = {datatypes.MP3Audio, datatypes.FLACAudio, datatypes.OGGAudio, datatypes.WAV}
    imports = {"librosa"}
    func = "librosa:load"
    output_instance = "numpy:ndarray"
    url_arg = "path"

    def _read(self, data, **kwargs):
        import librosa

        y, sr = librosa.load(data.url, **kwargs)
        return y, sr

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class SoundFileReader(FileReader):
    """Read audio into a NumPy array using ``soundfile``."""

    implements = {datatypes.FLACAudio, datatypes.OGGAudio, datatypes.WAV}
    imports = {"soundfile"}
    func = "soundfile:read"
    output_instance = "numpy:ndarray"
    url_arg = "file"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


# ---------------------------------------------------------------------------
# Video
# ---------------------------------------------------------------------------


class DecordVideoReader(FileReader):
    """Read a video file using ``decord`` (efficient random-access frame reader)."""

    implements = {datatypes.MP4Video, datatypes.WebMVideo}
    imports = {"decord"}
    func = "decord:VideoReader"
    output_instance = "decord:VideoReader"
    url_arg = "uri"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


# ---------------------------------------------------------------------------
# Images not yet covered
# ---------------------------------------------------------------------------


class PILImageReader(FileReader):
    """Read any PIL-supported image (WebP, BMP, GIF, HEIF, TIFF, PNG, JPEG…)
    into a NumPy array via Pillow."""

    implements = {
        datatypes.WebPImage,
        datatypes.BMPImage,
        datatypes.GIFImage,
        datatypes.HEIFImage,
        datatypes.PNG,
        datatypes.JPEG,
        datatypes.TIFF,
    }
    imports = {"pillow"}
    func = "PIL.Image:open"
    output_instance = "numpy:ndarray"
    url_arg = "fp"

    def _read(self, data, **kwargs):
        from PIL import Image
        import numpy as np
        import fsspec

        with fsspec.open(data.url, "rb", **(data.storage_options or {})) as f:
            img = Image.open(f)
            img.load()
        return np.array(img)

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class ImageIOReader(FileReader):
    """Read images (WebP, BMP, GIF, TIFF …) into NumPy arrays via imageio."""

    implements = {
        datatypes.WebPImage,
        datatypes.BMPImage,
        datatypes.GIFImage,
        datatypes.TIFF,
        datatypes.PNG,
        datatypes.JPEG,
    }
    imports = {"imageio"}
    func = "imageio:imread"
    output_instance = "numpy:ndarray"
    url_arg = "uri"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


# ---------------------------------------------------------------------------
# Geospatial
# ---------------------------------------------------------------------------


class KMLReader(FileReader):
    """Read KML/KMZ into a GeoDataFrame using geopandas/fiona."""

    implements = {datatypes.KMLFile}
    imports = {"geopandas", "fiona"}
    func = "geopandas:read_file"
    output_instance = "geopandas:GeoDataFrame"
    url_arg = "filename"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class GPXReader(FileReader):
    """Read GPX tracks/waypoints into a GeoDataFrame using geopandas/fiona."""

    implements = {datatypes.GPXFile}
    imports = {"geopandas", "fiona"}
    func = "geopandas:read_file"
    output_instance = "geopandas:GeoDataFrame"
    url_arg = "filename"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class MBTilesReader(FileReader):
    """Open an MBTiles file as an SQLite connection."""

    implements = {datatypes.MBTilesFile}
    imports = {"sqlite3"}
    func = "sqlite3:connect"
    output_instance = "sqlite3:Connection"
    url_arg = "database"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class OSMPBFReader(FileReader):
    """Parse an OpenStreetMap PBF extract with ``osmium`` (pyosmium)."""

    implements = {datatypes.OSMPBFFile}
    imports = {"osmium"}
    func = "osmium:FileProcessor"
    output_instance = "osmium:FileProcessor"
    url_arg = "path"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class LASReader(FileReader):
    """Read LAS/LAZ point cloud data using ``laspy``."""

    implements = {datatypes.LASFile}
    imports = {"laspy"}
    func = "laspy:read"
    output_instance = "laspy:LasData"
    url_arg = "source"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def discover(self, **kwargs):
        las = self.read(**kwargs)
        # Return a dict summarising the point cloud header metadata
        hdr = las.header
        return {
            "point_count": int(hdr.point_count),
            "point_format": int(las.point_format.id),
            "scales": list(hdr.scales),
            "offsets": list(hdr.offsets),
            "mins": list(hdr.mins),
            "maxs": list(hdr.maxs),
        }


class COGXarrayReader(FileReader):
    """Read a Cloud-Optimised GeoTIFF lazily via rioxarray."""

    implements = {datatypes.COGFile, datatypes.TIFF, datatypes.GDALRasterFile}
    imports = {"rioxarray"}
    func = "rioxarray:open_rasterio"
    output_instance = "xarray:DataArray"
    url_arg = "filename"
    storage_options = True

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def discover(self, **kwargs):
        return self.read(overview_level=0, **kwargs)


# ---------------------------------------------------------------------------
# Statistical / survey data
# ---------------------------------------------------------------------------


class PandasStataReader(Pandas):
    """Read a Stata .dta file into a pandas DataFrame."""

    implements = {datatypes.StataFile}
    func = "pandas:read_stata"
    url_arg = "filepath_or_buffer"


class PandasSPSSReader(Pandas):
    """Read an SPSS .sav file into a pandas DataFrame (pandas ≥ 1.2)."""

    implements = {datatypes.SPSSFile}
    func = "pandas:read_spss"
    url_arg = "path"


class PandasSASReader(Pandas):
    """Read a SAS .sas7bdat or .xpt file into a pandas DataFrame."""

    implements = {datatypes.SASFile}
    func = "pandas:read_sas"
    url_arg = "filepath_or_buffer"


class PyreadstatReader(FileReader):
    """Read SPSS/Stata/SAS files with full metadata using ``pyreadstat``."""

    implements = {datatypes.SPSSFile, datatypes.StataFile, datatypes.SASFile}
    imports = {"pyreadstat"}
    func = "pyreadstat:read_sav"
    output_instance = "pandas:DataFrame"
    url_arg = "filename_path"

    # dispatch to the right reader based on file extension
    _ext_to_func = {
        ".sav": "pyreadstat:read_sav",
        ".dta": "pyreadstat:read_dta",
        ".sas7bdat": "pyreadstat:read_sas7bdat",
        ".xpt": "pyreadstat:read_xport",
    }

    def _read(self, data, **kwargs):
        import pyreadstat

        ext = "." + data.url.rsplit(".", 1)[-1].lower()
        fn_name = self._ext_to_func.get(ext, "pyreadstat:read_sav").split(":")[1]
        fn = getattr(pyreadstat, fn_name)
        df, meta = fn(data.url, **kwargs)
        df.attrs["pyreadstat_meta"] = meta
        return df

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


# ---------------------------------------------------------------------------
# Bioinformatics / scientific
# ---------------------------------------------------------------------------


class BioPythonFASTAReader(FileReader):
    """Parse a FASTA file using ``Biopython`` into a list of SeqRecord objects."""

    implements = {datatypes.FASTAFile}
    imports = {"Bio"}
    func = "Bio.SeqIO:parse"
    output_instance = "builtins:list"
    url_arg = "handle"

    def _read(self, data, fmt="fasta", **kwargs):
        from Bio import SeqIO
        import fsspec

        with fsspec.open(data.url, "r", **(data.storage_options or {})) as f:
            return list(SeqIO.parse(f, fmt))

    def discover(self, **kwargs):
        records = self.read(**kwargs)
        return records[:10]


class BioPythonFASTQReader(BioPythonFASTAReader):
    """Parse a FASTQ file using Biopython."""

    implements = {datatypes.FASTQFile}

    def _read(self, data, fmt="fastq", **kwargs):
        return super()._read(data, fmt=fmt, **kwargs)


class PySAMVCFReader(FileReader):
    """Read a VCF file using ``pysam``."""

    implements = {datatypes.VCFFile}
    imports = {"pysam"}
    func = "pysam:VariantFile"
    output_instance = "pysam:VariantFile"
    url_arg = "filename"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def discover(self, **kwargs):
        vf = self.read(**kwargs)
        return {"header": str(vf.header), "samples": list(vf.header.samples)}


class PyBigWigReader(FileReader):
    """Read a BigWig coverage track using ``pyBigWig``."""

    implements = {datatypes.BigWigFile}
    imports = {"pyBigWig"}
    func = "pyBigWig:open"
    output_instance = "pyBigWig:pyBigWig"
    url_arg = "file"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def discover(self, **kwargs):
        bw = self.read(**kwargs)
        return {"chroms": bw.chroms(), "header": bw.header()}


class MDAnalysisReader(FileReader):
    """Load a molecular dynamics topology/trajectory using ``MDAnalysis``."""

    implements = {datatypes.PDBFile}
    imports = {"MDAnalysis"}
    func = "MDAnalysis:Universe"
    output_instance = "MDAnalysis.core.universe:Universe"
    url_arg = "filename"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def discover(self, **kwargs):
        u = self.read(**kwargs)
        return {
            "n_atoms": u.atoms.n_atoms,
            "n_residues": u.residues.n_residues,
            "n_segments": u.segments.n_segments,
        }


# ---------------------------------------------------------------------------
# Model formats
# ---------------------------------------------------------------------------


class ONNXReader(FileReader):
    """Load an ONNX model using ``onnx``."""

    implements = {datatypes.ONNXModel}
    imports = {"onnx"}
    func = "onnx:load"
    output_instance = "onnx.onnx_ml_pb2:ModelProto"
    url_arg = "f"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def discover(self, **kwargs):
        model = self.read(**kwargs)
        return {
            "ir_version": model.ir_version,
            "opset_version": model.opset_import[0].version if model.opset_import else None,
            "graph_inputs": [i.name for i in model.graph.input],
            "graph_outputs": [o.name for o in model.graph.output],
        }


class ONNXRuntimeReader(FileReader):
    """Create an ONNX Runtime inference session from a .onnx model."""

    implements = {datatypes.ONNXModel}
    imports = {"onnxruntime"}
    func = "onnxruntime:InferenceSession"
    output_instance = "onnxruntime:InferenceSession"
    url_arg = "path_or_bytes"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class TorchScriptReader(FileReader):
    """Load a TorchScript / PyTorch saved model using ``torch.jit.load``."""

    implements = {datatypes.TorchScriptModel}
    imports = {"torch"}
    func = "torch.jit:load"
    output_instance = "torch.jit:ScriptModule"
    url_arg = "f"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class TorchLoadReader(FileReader):
    """Load a plain PyTorch checkpoint (.pt / .pth) using ``torch.load``."""

    implements = {datatypes.TorchScriptModel}
    imports = {"torch"}
    func = "torch:load"
    output_instance = "builtins:object"
    url_arg = "f"
    other_funcs = {"torch.jit:load"}

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class JoblibReader(FileReader):
    """Load a joblib-serialised object."""

    implements = {datatypes.JoblibFile}
    imports = {"joblib"}
    func = "joblib:load"
    output_instance = "builtins:object"
    url_arg = "filename"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)


class NPZReader(FileReader):
    """Load a NumPy ``.npz`` archive into an ``NpzFile`` mapping."""

    implements = {datatypes.NPZFile}
    imports = {"numpy"}
    func = "numpy:load"
    output_instance = "numpy.lib.npyio:NpzFile"
    url_arg = "file"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def discover(self, **kwargs):
        npz = self.read(**kwargs)
        return {k: {"shape": v.shape, "dtype": str(v.dtype)} for k, v in npz.items()}


# ---------------------------------------------------------------------------
# Streaming services
# ---------------------------------------------------------------------------


class KafkaReader(BaseReader):
    """Stream records from a Kafka topic using ``confluent_kafka``."""

    implements = {datatypes.KafkaTopic}
    imports = {"confluent_kafka"}
    func = "confluent_kafka:Consumer"
    output_instance = "confluent_kafka:Consumer"

    def _read(
        self,
        data,
        group_id="intake-reader",
        auto_offset_reset="earliest",
        max_records=1000,
        **kwargs,
    ):
        from confluent_kafka import Consumer

        # URL format: kafka://broker:port/topic
        url = data.url
        if url.startswith("kafka://"):
            url = url[len("kafka://") :]
        broker, _, topic = url.partition("/")
        conf = {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
        }
        conf.update(kwargs)
        c = Consumer(conf)
        c.subscribe([topic])
        records = []
        while len(records) < max_records:
            msg = c.poll(1.0)
            if msg is None:
                break
            if not msg.error():
                records.append(msg.value())
        c.close()
        return records

    def discover(self, **kwargs):
        kwargs.setdefault("max_records", 5)
        return self.read(**kwargs)


class MQTTReader(BaseReader):
    """Subscribe to an MQTT topic and collect messages using ``paho-mqtt``."""

    implements = {datatypes.MQTTTopic}
    imports = {"paho"}
    func = "paho.mqtt.client:Client"
    output_instance = "builtins:list"

    def _read(self, data, max_messages=100, timeout=10, **kwargs):
        import paho.mqtt.client as mqtt
        import threading

        # URL format: mqtt://broker:port/topic
        url = data.url
        scheme = "mqtt"
        if url.startswith("mqtts://"):
            scheme = "mqtts"
            url = url[len("mqtts://") :]
        elif url.startswith("mqtt://"):
            url = url[len("mqtt://") :]
        broker_port, _, topic = url.partition("/")
        broker, _, port_str = broker_port.partition(":")
        port = int(port_str) if port_str else 1883

        messages = []
        done = threading.Event()

        def on_message(client, userdata, msg):
            messages.append(msg.payload)
            if len(messages) >= max_messages:
                done.set()

        client = mqtt.Client()
        if scheme == "mqtts":
            client.tls_set()
        client.on_message = on_message
        client.connect(broker, port)
        client.subscribe(topic)
        client.loop_start()
        done.wait(timeout=timeout)
        client.loop_stop()
        client.disconnect()
        return messages

    def discover(self, **kwargs):
        kwargs.setdefault("max_messages", 5)
        kwargs.setdefault("timeout", 5)
        return self.read(**kwargs)


class ArrowFlightReader(BaseReader):
    """Fetch a dataset from an Arrow Flight RPC service."""

    implements = {datatypes.ArrowFlightService}
    imports = {"pyarrow"}
    func = "pyarrow.flight:connect"
    output_instance = "pyarrow.lib:Table"

    def _read(self, data, descriptor=None, **kwargs):
        import pyarrow.flight as flight

        client = flight.connect(data.url)
        if descriptor is None:
            # list all flights and read the first
            flights = list(client.list_flights())
            if not flights:
                return None
            descriptor = flights[0].descriptor
        info = client.get_flight_info(descriptor)
        reader = client.do_get(info.endpoints[0].ticket)
        return reader.read_all()

    def discover(self, **kwargs):
        import pyarrow.flight as flight

        client = flight.connect(self.kwargs.get("data").url)
        return [f.descriptor.path for f in client.list_flights()]


# ---------------------------------------------------------------------------
# Database services
# ---------------------------------------------------------------------------


class InfluxDBReader(BaseReader):
    """Query an InfluxDB instance using ``influxdb-client``."""

    implements = {datatypes.InfluxDBService}
    imports = {"influxdb_client"}
    func = "influxdb_client:InfluxDBClient"
    output_instance = "pandas:DataFrame"

    def _read(self, data, query, token="", org="", **kwargs):
        from influxdb_client import InfluxDBClient

        client = InfluxDBClient(url=data.url, token=token, org=org)
        api = client.query_api()
        return api.query_data_frame(query)

    def discover(self, **kwargs):
        return {"url": self.kwargs.get("data").url, "type": "InfluxDB"}


class MongoDBReader(BaseReader):
    """Read a MongoDB collection using ``pymongo``."""

    implements = {datatypes.MongoDBService}
    imports = {"pymongo"}
    func = "pymongo:MongoClient"
    output_instance = "builtins:list"

    def _read(self, data, database, collection, query=None, limit=0, **kwargs):
        from pymongo import MongoClient

        client = MongoClient(data.url)
        col = client[database][collection]
        cursor = col.find(query or {})
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)

    def discover(self, **kwargs):
        kwargs.setdefault("limit", 5)
        return self.read(**kwargs)


class MongoDBPandasReader(MongoDBReader):
    """Read a MongoDB collection into a pandas DataFrame."""

    imports = {"pymongo", "pandas"}
    output_instance = "pandas:DataFrame"

    def _read(self, data, database, collection, query=None, limit=0, **kwargs):
        import pandas as pd

        records = super()._read(data, database, collection, query=query, limit=limit, **kwargs)
        return pd.DataFrame(records)


class ElasticsearchReader(BaseReader):
    """Read documents from an Elasticsearch / OpenSearch index using ``elasticsearch-py``."""

    implements = {datatypes.ElasticsearchService}
    imports = {"elasticsearch"}
    func = "elasticsearch:Elasticsearch"
    output_instance = "builtins:list"

    def _read(self, data, index, query=None, size=1000, **kwargs):
        from elasticsearch import Elasticsearch

        es = Elasticsearch(data.url)
        resp = es.search(index=index, body=query or {"query": {"match_all": {}}}, size=size)
        return [hit["_source"] for hit in resp["hits"]["hits"]]

    def discover(self, **kwargs):
        kwargs.setdefault("size", 5)
        return self.read(**kwargs)


class RedisReader(BaseReader):
    """Read keys/values from a Redis store using ``redis-py``."""

    implements = {datatypes.RedisService}
    imports = {"redis"}
    func = "redis:Redis"
    output_instance = "builtins:dict"

    def _read(self, data, keys="*", **kwargs):
        import redis

        r = redis.from_url(data.url)
        matched = r.keys(keys)
        return {k.decode(): r.get(k) for k in matched}

    def discover(self, **kwargs):
        import redis

        r = redis.from_url(self.kwargs.get("data").url)
        return {"dbsize": r.dbsize(), "info": r.info("server")}


class SocrataReader(BaseReader):
    """Read data from a Socrata Open Data portal using ``sodapy``."""

    implements = {datatypes.SocrataService}
    imports = {"sodapy"}
    func = "sodapy:Socrata"
    output_instance = "pandas:DataFrame"

    def _read(self, data, limit=10000, **kwargs):
        from sodapy import Socrata
        import pandas as pd

        # URL: https://data.domain.gov/resource/xxxx-xxxx.json
        import re

        m = re.search(r"https?://([^/]+)/resource/([^/?]+)", data.url)
        if not m:
            raise ValueError(f"Cannot parse Socrata URL: {data.url}")
        domain, dataset_id = m.group(1), m.group(2).split(".")[0]
        client = Socrata(domain, None)
        return pd.DataFrame(client.get(dataset_id, limit=limit, **kwargs))

    def discover(self, **kwargs):
        kwargs.setdefault("limit", 10)
        return self.read(**kwargs)


class OGCAPIFeaturesReader(BaseReader):
    """Read features from an OGC API – Features endpoint into a GeoDataFrame."""

    implements = {datatypes.OGCAPIFeatures}
    imports = {"geopandas", "requests"}
    func = "geopandas:read_file"
    output_instance = "geopandas:GeoDataFrame"

    def _read(self, data, limit=10000, **kwargs):
        import geopandas as gpd

        url = data.url
        if "?" not in url:
            url = f"{url}?f=json&limit={limit}"
        return gpd.read_file(url, **kwargs)

    def discover(self, **kwargs):
        import geopandas as gpd

        url = self.kwargs.get("data").url
        return gpd.read_file(f"{url}?f=json&limit=10")


# ---------------------------------------------------------------------------
# NPZ arrays (NumPy archive)
# ---------------------------------------------------------------------------


class NPZDaskReader(FileReader):
    """Load a NumPy .npz archive lazily as a dict of dask arrays."""

    implements = {datatypes.NPZFile}
    imports = {"dask", "numpy"}
    func = "dask.array:from_delayed"
    output_instance = "builtins:dict"
    url_arg = "file"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def _read(self, data, **kwargs):
        import dask.array as da
        import numpy as np

        npz = np.load(data.url, allow_pickle=False)
        return {k: da.from_array(npz[k], chunks="auto") for k in npz.files}

    def discover(self, **kwargs):
        npz_dict = self.read(**kwargs)
        return {k: {"shape": v.shape, "dtype": str(v.dtype)} for k, v in npz_dict.items()}


# ---------------------------------------------------------------------------
# Scientific / medical imaging readers
# ---------------------------------------------------------------------------


def _as_local(data):
    """Return a local filesystem path for *data*, fetching remote URLs if needed.

    Many imaging libraries (pynrrd, SimpleITK, openslide, …) only accept a real
    local path.  For remote/fsspec URLs we transparently cache the file locally
    via ``simplecache``.
    """
    url = data.url
    if data.storage_options or "://" in url or "::" in url:
        return fsspec.open_local(
            f"simplecache::{url}", **{"simplecache": {}, **(data.storage_options or {})}
        )
    return url


class NRRDReader(FileReader):
    """Read an NRRD volume with ``pynrrd``.

    ``nrrd.read`` returns a ``(data, header)`` tuple; we expose the array and
    surface the header via :meth:`discover`.
    """

    implements = {datatypes.NRRD}
    imports = {"pynrrd"}
    func = "nrrd:read"
    output_instance = "numpy:ndarray"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def _read(self, data, **kw):
        arr, _header = self._func(_as_local(data), **kw)
        return arr

    def discover(self, **kw):
        import nrrd

        header = nrrd.read_header(_as_local(self.data))
        return header


class SimpleITKReader(FileReader):
    """Read NRRD / MetaImage (and most ITK formats) using ``SimpleITK``.

    Produces a ``SimpleITK.Image``; use ``SimpleITK.GetArrayFromImage`` to get
    a NumPy view.
    """

    implements = {datatypes.MetaImage, datatypes.NRRD}
    imports = {"SimpleITK"}
    func = "SimpleITK:ReadImage"
    output_instance = "SimpleITK:Image"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def _read(self, data, **kw):
        return self._func(_as_local(data), **kw)

    def discover(self, **kw):
        img = self.read(**kw)
        return {
            "size": tuple(img.GetSize()),
            "spacing": tuple(img.GetSpacing()),
            "components": img.GetNumberOfComponentsPerPixel(),
            "pixel_type": img.GetPixelIDTypeAsString(),
        }


class OpenEXRReader(FileReader):
    """Read an OpenEXR image as a NumPy array via ``imageio``."""

    implements = {datatypes.OpenEXRImage}
    imports = {"imageio"}
    func = "imageio.v3:imread"
    output_instance = "numpy:ndarray"
    url_arg = "uri"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def _read(self, data, **kw):
        return self._func(_as_local(data), **kw)


class OpenSlideReader(FileReader):
    """Open a whole-slide pathology image with ``openslide-python``.

    Returns an ``OpenSlide`` handle, from which pyramidal levels and region
    reads are available without loading the (often multi-gigabyte) image into
    memory.
    """

    implements = {datatypes.WholeSlideImage}
    imports = {"openslide-python"}
    func = "openslide:OpenSlide"
    output_instance = "openslide:OpenSlide"
    url_arg = "filename"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def _read(self, data, **kw):
        return self._func(_as_local(data), **kw)

    def discover(self, **kw):
        slide = self.read(**kw)
        return {
            "dimensions": slide.dimensions,
            "level_count": slide.level_count,
            "level_dimensions": slide.level_dimensions,
            "properties": dict(slide.properties),
        }


class TiffSlideReader(FileReader):
    """Read a whole-slide (or ordinary) TIFF as a NumPy array with ``tifffile``.

    A lighter-weight alternative to OpenSlide that works for the TIFF-based
    slide formats (Aperio ``.svs``, ``.qptiff``, Zeiss ``.lsm``, …).
    """

    implements = {datatypes.WholeSlideImage}
    imports = {"tifffile"}
    func = "tifffile:imread"
    output_instance = "numpy:ndarray"
    url_arg = "files"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def _read(self, data, **kw):
        return self._func(_as_local(data), **kw)


class ImageIOVideoReader(FileReader):
    """Open a video (AVI, …) as an ``imageio`` reader for frame iteration."""

    implements = {datatypes.AVIVideo, datatypes.MP4Video, datatypes.WebMVideo}
    imports = {"imageio"}
    func = "imageio:get_reader"
    output_instance = "imageio.core.format:Reader"
    url_arg = "uri"

    @classmethod
    def is_ok(cls, data) -> bool:
        return cls._url_is_single(data)

    def _read(self, data, **kw):
        return self._func(_as_local(data), **kw)

    def discover(self, **kw):
        reader = self.read(**kw)
        meta = dict(reader.get_meta_data())
        # convert non-serialisable values to str where necessary
        return {
            k: (v if isinstance(v, (int, float, str, bool)) else str(v)) for k, v in meta.items()
        }


def reader_from_call(func: str, *args, join_lines=False, **kwargs) -> BaseReader:
    """Attempt to construct a reader instance by finding one that matches the function call

    Fails for readers that don't define a func, probably because it depends on the file
    type or needs a dynamic instance to be a method of.

    Parameters
    ----------
    func: callable | str
        If a callable, pass args and kwargs as you would have done to execute the function.
        If a string, it should look like ``"func(arg1, args2, kwarg1, **kw)"``, i.e., a normal
        python call but as a string. In the latter case, args and kwargs are ignored
    """

    import re
    from itertools import chain

    if isinstance(func, str):
        if join_lines:
            func = func.replace("\n", "")
        frame = inspect.currentframe().f_back
        match = re.match("^([^(]+=)?([^(]+)[(](.*)[)][^)]?$", func)
        if match:
            groups = match.groups()
        else:
            raise ValueError
        func = eval(groups[1], frame.f_globals, frame.f_locals)
        args, kwargs = eval(
            f"""(lambda *args, **kwargs: (args, kwargs))({groups[2]})""",
            frame.f_globals,
            frame.f_locals,
        )

    package = func.__module__.split(".", 1)[0]

    found = False
    for cls in subclasses(BaseReader):
        if cls.check_imports() and any(
            f.split(":", 1)[0].split(".", 1)[0] == package for f in ({cls.func} | cls.other_funcs)
        ):
            ffs = [f for f in ({cls.func} | cls.other_funcs) if import_name(f) == func]
            if ffs:
                found = cls
                func_name = ffs[0]
                break
    if not found:
        raise ValueError("Function not found in the set of readers")

    pars = inspect.signature(func).parameters
    kw = dict(zip(pars, args), **kwargs)
    data_kw = {}
    if issubclass(cls, FileReader):
        data_kw["storage_options"] = kw.pop("storage_options", None)
        data_kw["url"] = kw.pop(getattr(cls, "other_urls", {}).get(func_name, cls.url_arg))
    datacls = None
    if len(cls.implements) == 1:
        datacls = next(iter(cls.implements))
    elif getattr(cls, "url_arg", None):
        clss = datatypes.recommend(data_kw["url"], storage_options=data_kw["storage_options"])
        clss2 = [c for c in clss if c in cls.implements]
        if clss:
            datacls = next(iter(chain(clss2, clss)))
    if datacls:
        datacls = datacls(**data_kw)
        if data_kw["storage_options"] is None:
            del data_kw["storage_options"]
        cls = cls(datacls, **kwargs)
    else:
        url = data_kw.pop("url")
        cls = cls(url, **data_kw)

    return cls
