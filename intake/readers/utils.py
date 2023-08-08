import re
from hashlib import md5
from itertools import zip_longest
from typing import Any, Callable, Iterable

from intake import import_name


def subclasses(cls: type) -> set:
    """Find all direct and indirect subclasses"""
    out = set()
    for cl in cls.__subclasses__():
        out.add(cl)
        out |= subclasses(cl)
    return out


def merge_dicts(*dicts: tuple[dict, ...]) -> dict:
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


def _func_to_str(f: Callable) -> str:
    from intake.readers.readers import BaseReader

    if isinstance(f, BaseReader):
        return "{data(%s)}" % f.to_entry().token
    if isinstance(f, Tokenizable):
        # TODO: not covered, probably wrong, may need separate Data, DataDescription
        #  and
        return "{%s}" % f.token
    return "{func(%s)}" % f"{f.__module__}:{f.__name__}"


def find_funcs(val):
    """Walk nested dict/iterables, replacing functions with string package.mod:func form"""
    if isinstance(val, dict):
        return {k: find_funcs(v) for k, v in val.items()}
    elif isinstance(val, (str, bytes)):
        return val
    elif isinstance(val, Iterable):
        return type(val)([find_funcs(v) for v in val])
    elif callable(val):
        return _func_to_str(val)
    else:
        return val


def find_readers(val, out=None):
    """Walk nested dict/iterables, finding all BaseReader instances"""
    from intake.readers.readers import BaseReader

    if out is None:
        out = set()
    if isinstance(val, dict):
        [find_readers(v, out) for k, v in val.items()]
    elif isinstance(val, (str, bytes)):
        pass
    elif isinstance(val, Iterable):
        [find_readers(v, out) for v in val]
    elif isinstance(val, BaseReader):
        out.add(val)
        # recurse to find readers depending on more readers
        find_readers(val.__dict__, out)
    return out


class Tokenizable:
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
        return {k: find_funcs(v) for k, v in self.__dict__.items() if not k.startswith("_") and k not in self._avoid}

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
        return type(self) == type(other) and self.token == other.token

    @classmethod
    def qname(cls):
        """module:class name of this class, makes str for import_name"""
        return f"{cls.__module__}:{cls.__name__}"

    def to_dict(self):
        return to_dict(self)

    @classmethod
    def from_dict(cls, data):
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


def descend_to_path(path: str | list, kwargs: dict | list | tuple):
    if isinstance(path, str):
        path = path.split(".")
    part = path.pop(0)
    if part.isnumeric():
        part = int(part)
    if path:
        return descend_to_path(path, kwargs[part])
    return kwargs[part]


def extract_by_path(path: str, cls: type, name: str, kwargs: dict) -> tuple:
    """Walk kwargs, replacing dotted keys in path by UserParameter template"""
    value = descend_to_path(path, kwargs)
    up = cls(default=value)
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


def extract_by_value(value: Any, cls: type, name: str, kwargs: dict) -> tuple:
    """Walk kwargs, replacing given value with UserParameter placeholder"""
    up = cls(default=value)
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
            return needle
        elif isinstance(val, (str, bytes)):
            return val.replace(needle, replace)
    except (ValueError, TypeError):
        pass
    return val
