from __future__ import annotations

import importlib.metadata
import re
from functools import cache
from typing import Iterable

from intake.readers.utils import Completable, subclasses


class Namespace(Completable):
    """A set of functions as an accessor on a Reader, producing a Pipeline"""

    acts_on = ()
    imports = "nolibrary"

    def __init__(self, reader):
        self.reader = reader

    @classmethod
    @cache
    def check_imports(cls):
        """See if required packages are importable, but don't import them"""
        # TODO: this is copied from readers.py, should refactor to utils
        try:
            importlib.metadata.distribution(cls.imports)
            return True
        except (ImportError, ModuleNotFoundError, NameError):
            return False

    @classmethod
    @cache
    def _funcs(cls) -> Iterable[str]:
        if not cls.check_imports():
            return []
        # if self.reader.output_instance doesn't match self.acts_on
        cls.mod = importlib.import_module(cls.imports)
        return [f for f in dir(cls.mod) if callable(getattr(cls.mod, f)) and not f.startswith("_")]

    def __dir__(self) -> Iterable[str]:
        # if self.reader.output_instance doesn't match self.acts_on:
        # return []
        return self._funcs()

    def __getattr__(self, item):
        super().tab_completion_fixer(item)
        try:
            dir(self)
            func = getattr(self.mod, item)
            return FuncHolder(self.reader, func)
        except RecursionError as e:
            raise AttributeError from e

    def __repr__(self):
        return f"{self.imports} namespace"


class FuncHolder:
    """Acts like a function to capture a call into a pipeline stage"""

    def __init__(self, reader, func):
        self.reader = reader
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.reader.apply(self.func, **kwargs)


class np(Namespace):
    acts_on = (".*",)  # numpy works with a wide variety of objects
    imports = "numpy"


class ak(Namespace):
    acts_on = "awkward:Array", "dask_awkward:Array"
    imports = "awkward"


class xr(Namespace):
    acts_on = "xarray:DataArray", "xarray:Dataset"
    imports = "xarray"


class pd(Namespace):
    acts_on = ("pandas:DataFrame",)
    imports = "pandas"


def get_namespaces(reader):
    """These namespaces are available on the reader"""
    out = {}
    for space in subclasses(Namespace):
        if any(re.match(act, reader.output_instance) for act in space.acts_on):
            out[space.__name__] = space(reader)
    return out
