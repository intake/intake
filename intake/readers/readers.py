import importlib.metadata

from intake import import_name
from intake.readers import datatypes
from intake.readers.utils import subclasses


class BaseReader:
    imports = set()  # allow for pip-style versions maybe
    implements = set()
    optional_imports = set()
    func = "builtins:NotImplementedError"
    concat_func = None
    output_instance = None
    url_arg = None
    storage_options = False

    def __init__(self, data):
        self.data = data

    @classmethod
    def check_imports(cls):
        for package in cls.imports:
            importlib.metadata.distribution(package)

    @classmethod
    def doc(cls):
        func = import_name(cls.func)
        return func.__doc__

    def glipse(self, **kwargs):
        """Minimal snapshot of the data"""
        raise NotImplementedError

    def read(self, **kwargs):
        kw = self.data.kwargs.copy()
        kw.update(kwargs)
        func = import_name(self.func)
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

        return func(**kw)


class Pandas(BaseReader):
    imports = {"pandas"}
    concat_func = "pandas:concat"
    output_instance = "pandas:DataFrame"


class PandasParquet(Pandas):
    concat_func = None  # pandas concats for us
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "pandas:read_parquet"
    url_arg = "path"


class PandasCSV(Pandas):
    implements = {datatypes.CSV}
    func = "pandas:read_csv"
    url_arg = "filepath_or_buffer"

    def glipse(self, **kwargs):
        kw = {"nrows": 10, self.url_arg: self.data.filelist[0]}
        kw.update(kwargs)
        return self.read(**kw)


def recommend(data):
    out = set()
    for cls in subclasses(BaseReader):
        if type(data) in cls.implements:
            out.add(cls)
    return out
