from __future__ import annotations

import importlib.metadata
import numbers
import re
import typing
from functools import lru_cache as cache
from hashlib import md5
from itertools import zip_longest
from typing import Any, Iterable, Mapping

from intake import import_name


class SecurityError(RuntimeError):
    """The given operation is disabled in the Intake config"""


def subclasses(cls: type) -> set:
    """Find all direct and indirect subclasses

    Most recently created and most specialised classes come first
    """
    out = set()
    for cl in reversed(cls.__subclasses__()):
        # TODO: if cls.check_imports exists and returns False, do we descend?
        out |= subclasses(cl)
        out.add(cl)
    return out


def merge_dicts(*dicts: dict) -> dict:
    """Deep-merge dictionary values, latest value wins

    Examples
    --------
    >>> merge_dicts({"a": {"a": 0, "b": 1}}, {"a": {"a": 1}, "b": 1})
    {"a": {"a": 1, "b": 1}}, "b": 1)

    >>> merge_dicts({"a": [None, True]}, {"a": [False, None]})
    {"a": [False, True}
    """
    if isinstance(dicts[0], dict):
        out = {}
        for dic in dicts:
            for k, v in dic.items():
                if k in out and isinstance(v, Iterable) and not isinstance(v, (bytes, str)):
                    # deep-merge nested dicts
                    out[k] = merge_dicts(out[k], v)
                else:
                    out[k] = v
    elif isinstance(dicts[0], Iterable) and not isinstance(dicts[0], (bytes, str)):
        stuff = []
        for values in zip_longest(*(d for d in dicts if d is not None)):
            stuff.append(merge_dicts(*values))

        out = type(dicts[0])(stuff)
    else:
        out = next((d for d in dicts if d is not None), None)

    return out


def nested_keys_to_dict(kw: dict[str, Any]) -> dict:
    """Nest keys of the form "field.subfield.item" into dicts

    Examples
    --------
    >>> nested_keys_to_dict({"field": 0, "deeper.field": 1, "deeper.other": 2, "deep.est.field": 3})
    {'field': 0, 'deeper': {'field': 1, 'other': 2}, 'deep': {'est': {'field': 3}}}

    >>>  nested_keys_to_dict({"deeper.1.field": 1, "list.1.1.1": True, "list.1.0": False})
    {'deeper': [None, {"field": 1}], "list": [None, [False, [None, True]]]}
    """
    out = {}
    for k, v in kw.items():
        bits = k.split(".")
        o = out
        for bit, bit2 in zip(bits[:-1], bits[1:]):
            if bit2.isnumeric():
                bit2 = int(bit2)
                newpart = [None] * (int(bit2) + 1)
            else:
                newpart = {}
            if isinstance(o, dict):
                o = o.setdefault(bit, newpart)
            else:
                if o[int(bit)] is None:
                    o[int(bit)] = newpart
                o = o[int(bit)]
        bit = bits[-1]
        if bit.isnumeric():
            bit = int(bit)
        o[bit] = v
    return out


func_or_method = re.compile(r"<(function|method) ([^ ]+) at 0x[0-9a-f]+>")


def find_funcs(val, tokens={}):
    """Walk nested dict/iterables, replacing functions with string package.mod:func form"""
    import base64
    import pickle

    from intake.readers import BaseData, BaseReader

    if isinstance(val, dict):
        return {k: find_funcs(v, tokens=tokens) for k, v in val.items()}
    elif isinstance(val, (str, bytes)):
        return val
    elif isinstance(val, Iterable):
        # list, tuple, set-like
        return type(val)([find_funcs(v, tokens=tokens) for v in val])
    if isinstance(val, (BaseReader, BaseData)):
        ent = val.to_entry()
        find_funcs(ent, tokens)
        tok = ent.token
        tokens[tok] = val
        return "{data(%s)}" % tok
    if isinstance(val, Tokenizable):
        return val.to_dict()
    elif callable(val):
        name = "{func(%s)}" % f"{val.__module__}:{val.__name__}"
        if "<locals>" in name or "__main__" in name or getattr(val, "__closure__", False):
            raise RuntimeError("Cannot store dynamically defined function: %s", val)
        return name
    elif val is None or isinstance(val, (numbers.Number, BaseData, BaseReader)):
        return val
    else:
        return "{pickle64(%s)}" % base64.b64encode(pickle.dumps(val)).decode()


class LazyDict(Mapping):
    """Subclass this to make lazy dictionaries, where getting values can be expensive"""

    _keys = None

    def __getitem__(self, item):
        raise NotImplementedError

    def __len__(self):
        return len(self.keys())

    def keys(self):
        if self._keys is None:
            self._keys = set(self)
        return self._keys

    def __contains__(self, item):
        return item in self.keys()

    def __iter__(self):
        raise NotImplementedError


class PartlyLazyDict(LazyDict):
    """A dictionary-like, where some components may be lazy"""

    def __init__(self, *mappings):
        self.dic = {}
        self.lazy = []
        for mapping in mappings:
            if isinstance(mapping, LazyDict):
                self.lazy.append(mapping)
            else:
                self.dic.update(mapping)

    def keys(self):
        out = set(self.dic)
        for mapping in self.lazy:
            out.update(mapping)
        return out

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return iter(self.keys())

    def __getitem__(self, item):
        if item in self.dic:
            return self.dic[item]
        for mapping in self.lazy:
            if item in mapping:
                return mapping[item]
        raise KeyError(item)

    def __setitem__(self, key, value):
        self.dic[key] = value

    def update(self, data):
        if isinstance(data, LazyDict):
            self.lazy.append(data)
        else:
            self.dic.update(data)

    def copy(self):
        return PartlyLazyDict(self.dic.copy(), *self.lazy)


class FormatWithPassthrough(dict):
    """When calling .format(), use this to replace only those keys that are found"""

    def __getitem__(self, item):
        try:
            return super().__getitem__(item)
        except KeyError:
            return "{%s}" % item


@cache
def check_imports(*imports: Iterable[str]) -> bool:
    """See if required packages are importable, but don't import them"""
    import sys

    try:
        for package in imports:
            if package:
                package in sys.modules or importlib.metadata.distribution(package)
        return True
    except (ImportError, ModuleNotFoundError, NameError):
        return False


class Completable:
    """Helper mixin for classes with dynamic tab completion"""

    @classmethod
    @cache
    def check_imports(cls):
        return check_imports(*getattr(cls, "imports"))

    @staticmethod
    def tab_completion_fixer(item):
        # just make this a function?
        if item in {
            "_ipython_key_completions_",
            "_ipython_display_",
            "__wrapped__",
            "_ipython_canary_method_should_not_exist_",
            "_render_traceback_",
        }:
            raise AttributeError
        if item.startswith("_repr_"):
            raise AttributeError


class Tokenizable(Completable):
    """Provides reliable hash/eq support to classes that hold dict attributes

    The convention is, that attributes starting with _ are not included in the
    hash and so can be mutated. Changing a non-_ attribute should only be done
    when making a new instance.
    """

    _tok = None
    _avoid = {"metadata"}

    def _dic_for_comp(self):
        # TODO: we don't consider metadata part of the token. Any others?
        #  Do we just want to exclude others?
        return {
            k: find_funcs(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in self._avoid
        }

    def _token(self):
        dic = self._dic_for_comp()
        dictxt = func_or_method.sub(r"\2", str(dic))
        return md5(f"{self.qname()}|{dictxt}".encode()).hexdigest()[:16]

    @property
    def token(self):
        """Token is computed from all non-_ attributes and then cached.

        Even if those attributes are mutated, the token will not change, but the
        resultant might or might not beequal to the original.
        """
        if self._tok is None:
            self._tok = self._token()
        return self._tok

    def __hash__(self):
        """Hash depends on class name and all non-_* attributes"""
        return int(self.token, 16)

    def __eq__(self, other):
        return type(self) == type(other) and (
            self.token == other.token or self.to_dict() == other.to_dict()
        )

    @classmethod
    def qname(cls):
        """package.module:class name of this class, makes str for import_name"""
        return f"{cls.__module__}:{cls.__name__}"

    def to_dict(self):
        """Dictionary representation of the instances contents"""
        return to_dict(self)

    def pprint(self):
        """Produce nice text formatting of the instance's contents"""
        from pprint import pp

        pp(self.to_dict())

    @classmethod
    def from_dict(cls, data):
        """Recreate instance from the results of to_dict()"""
        data = data.copy()
        if "cls" in data:
            cls = import_name(data.pop("cls"))
        obj = object.__new__(cls)
        obj.__dict__.update(data)  # walk data
        return obj


def to_dict(thing):
    """Serialise deep structure into JSON-like hierarchy, invoking to_dict() on intake instances"""
    if isinstance(thing, dict):
        return {k: to_dict(v) for k, v in thing.items()}
    elif isinstance(thing, (tuple, list)):
        return [to_dict(v) for v in thing]
    elif isinstance(thing, Tokenizable):
        return {k: to_dict(v) for k, v in thing.__dict__.items() if not k.startswith("_")}
    else:
        return thing


def make_cls(cls: str | type, kwargs: dict):
    """Recreate class instance from class/kwargs pair"""
    if isinstance(cls, str):
        cls = import_name(cls)
    return cls(**kwargs)


def descend_to_path(path: str | list, kwargs: dict | list | tuple, name: str = ""):
    """Find the value at the location `path` in the deeply nested dict `kwargs`

    If a name is given, replace that value by "{name}" - used by parameter
    extraction.
    """
    if isinstance(path, str):
        path = path.split(".")
    part = path.pop(0)
    if part.isnumeric():
        part = int(part)
    if path:
        return descend_to_path(path, kwargs[part], name)
    out = kwargs[part]
    if name:
        kwargs[part] = "{%s}" % name
    return out


def extract_by_path(path: str, cls: type, name: str, kwargs: dict, **kw) -> tuple:
    """Walk kwargs, replacing dotted keys in path by UserParameter template"""
    value = descend_to_path(path, kwargs, name)
    up = cls(default=value, **kw)
    return merge_dicts(kwargs, nested_keys_to_dict({path: "{%s}" % name})), up


def _by_value(val, up, name):
    if isinstance(val, dict):
        return {k: _by_value(v, up, name) for k, v in val.items()}
    elif isinstance(val, Iterable) and not isinstance(val, (str, bytes)):
        return type(val)([_by_value(v, up, name) for v in val])
    elif isinstance(val, str) and isinstance(up.default, str) and up.default in val:
        return val.replace(up.default, "{%s}" % name)
    else:
        try:
            if isinstance(val, (str, bytes)) and up.default in val:
                return val.replace(up.default, "{%s}" % name)
            if val == up.default:
                return "{%s}" % name
        except (TypeError, ValueError):
            pass
        return val


def extract_by_value(value: Any, cls: type, name: str, kwargs: dict, **kw) -> tuple:
    """Walk kwargs, replacing given value with UserParameter placeholder"""
    up = cls(default=value, **kw)
    kw = _by_value(kwargs, up, name)
    return kw, up


def replace_values(val, needle, replace):
    """Find `needle` in the given values and replace with `replace`

    Useful for removing sensitive values from kwargs and replacing with functions
    like "{env(...)}".
    """
    if isinstance(val, dict):
        return {k: replace_values(v, needle, replace) for k, v in val.items()}
    elif isinstance(val, Iterable) and not isinstance(val, (str, bytes)):
        return type(val)([replace_values(v, needle, replace) for v in val])
    try:
        if val == needle:
            return replace
        elif isinstance(val, (str, bytes)):
            return val.replace(needle, replace)
    except (ValueError, TypeError):
        pass
    return val


def one_to_one(it: Iterable) -> dict:
    return {_: _ for _ in it}


def all_to_one(it: Iterable, one: Any) -> dict:
    return {_: one for _ in it}


s1 = re.compile("(.)([A-Z][a-z]+)")
s2 = re.compile("([a-z0-9])([A-Z])")


@cache
def camel_to_snake(name: str) -> str:
    # https://stackoverflow.com/a/1176023/3821154
    name = s1.sub(r"\1_\2", name)
    return s2.sub(r"\1_\2", name).lower()


@cache
def snake_to_camel(name: str) -> str:
    # https://stackoverflow.com/a/1176023/3821154
    return "".join(word.title() for word in name.split("_"))


def pattern_to_glob(pattern: str) -> str:
    """
    Convert a path-as-pattern into a glob style path

    Uses the pattern's indicated number of '?' instead of '*' where an int was specified.

    Parameters
    ----------
    pattern : str
        Path as pattern optionally containing format_strings

    Returns
    -------
    glob_path : str
        Path with int format strings replaced with the proper number of '?' and '*' otherwise.

    Examples
    --------
    >>> pattern_to_glob('{year}/{month}/{day}.csv')
    '*/*/*.csv'
    >>> pattern_to_glob('{year:4}/{month:2}/{day:2}.csv')
    '????/??/??.csv'
    >>> pattern_to_glob('data/{year:4}{month:02}{day:02}.csv')
    'data/????????.csv'
    >>> pattern_to_glob('data/*.csv')
    'data/*.csv'
    """
    # https://github.com/intake/intake/issues/776#issuecomment-1917737732 by @JessicaS11
    from string import Formatter

    fmt = Formatter()
    glob_path = ""
    for literal_text, field_name, format_specs, _ in fmt.parse(format_string=pattern):
        glob_path += literal_text
        if field_name and (glob_path != "*"):
            try:
                glob_path += "?" * int(format_specs)
            except ValueError:
                glob_path += "*"
    return glob_path


def safe_dict(x):
    """Make a dict or list-like int a form you can JSON serialize"""
    if isinstance(x, str):
        return x
    if isinstance(x, typing.Mapping):
        return {k: safe_dict(v) for k, v in x.items()}
    if isinstance(x, typing.Iterable):
        return [safe_dict(v) for v in x]
    return str(x)


def port_in_use(host, port=None):
    import socket

    if isinstance(host, tuple):
        host, port = host
    elif isinstance(host, str) and host.startswith("http"):
        from urllib.parse import urlparse

        parsed = urlparse(host)
        host = parsed.hostname
        if parsed.port is None:
            port = port

    if (port is None) or (host is None):
        raise ValueError(f"host={host} and port={port} is not valid.")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, int(port)))
        s.shutdown(2)
        return True
    except:  # noqa: E722
        return False


def find_free_port():
    import socket
    from contextlib import closing

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]
