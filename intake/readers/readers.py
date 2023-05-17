import importlib.metadata

from intake import import_name
from intake.readers import filetypes


class BaseReader:
    imports = set()  # allow for pip-style versions maybe
    implements = None
    optional_imports = set()
    func = "builtins:NotImplementedError"
    url_arg = None
    storage_options = False

    @classmethod
    def check_imports(cls):
        for package in cls.imports:
            importlib.metadata.distribution(package)

    @classmethod
    def doc(cls):
        func = import_name(cls.func)
        return func.__doc__

    @classmethod
    def read(cls, **kwargs):
        func = import_name(cls.func)
        return func(**kwargs)


class PandasCSV(BaseReader):
    implements = filetypes.Parquet
    imports = {"pandas"}
    func = {"pandas.read_csv"}
    url_arg = "filepath_or_buffer"
    storage_options = True
