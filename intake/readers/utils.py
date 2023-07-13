import re
from hashlib import md5
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
    merge_dicts({"a": {"a": 0, "b": 1}}, {"a": {"a": 2}, "b": 3})
    """
    out = {}
    for dic in dicts:
        for k, v in dic.items():
            if k in out and isinstance(v, dict) and isinstance(out[k], dict):
                # deep-merge nested dicts
                out[k] = merge_dicts(out[k], v)
            else:
                out[k] = v
    return out


def nested_keys_to_dict(kw: dict[str, Any]) -> dict:
    """Nest keys of the form "field.subfield.item" into dicts

    Examples
    --------
    >>> nested_keys_to_dict({"field": 0, "deeper.field": 1, "deeper.other": 2, "deep.est.field": 3})
    {'field': 0, 'deeper': {'field': 1, 'other': 2}, 'deep': {'est': {'field': 3}}}
    """
    out = {}
    for k, v in kw.items():
        bits = k.split(".")
        o = out
        for bit in bits[:-1]:
            o = o.setdefault(bit, {})
        o[bits[-1]] = v
    return out


func_or_method = re.compile(r"<(function|method) ([^ ]+) at 0x[0-9a-f]+>")


def _func_to_str(f: Callable) -> str:
    if isinstance(f, Tokenizable):
        return f"{f.token}"
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


class Tokenizable:
    """Provides reliable hash/eq support to classes that hold dict attributes

    The convention is, that attributes starting with _ are not included in the
    hash and so can be mutated. Changing a non-_ attribute should only be done
    when making a new instance.
    """

    _tok = None

    @property
    def token(self):
        # TODO: this effectively says that mutation, if allowed, does not change token
        #  implying that only _ attributes are multable, such as _metadata
        if self._tok is None:
            # TODO: this is not necessary for Descriptions where funcs have already been stringified
            dic = {k: find_funcs(v) for k, v in self.__dict__.items() if not k.startswith("_")}
            dictxt = func_or_method.sub(r"\2", str(dic))
            self._tok = md5(f"{self.qname()}|{dictxt}".encode()).hexdigest()[:16]
        return self._tok

    def __hash__(self):
        """Hash depends on class name and all non-_* attributes"""
        return int(self.token, 16)

    def __eq__(self, other):
        return self.token == other.token

    @classmethod
    def qname(cls):
        """module:class name of this class, makes str for import_name"""
        return f"{cls.__module__}:{cls.__name__}"


def make_cls(cls: str | type, kwargs: dict):
    """Recreate class instance from class/kwargs pair"""
    if isinstance(cls, str):
        cls = import_name(cls)
    return cls(**kwargs)


def extract_by_path(path: str, cls: type, name: str, kwargs: dict) -> tuple:
    """Walk kwargs, replacing dotted keys in path by UserParameter template"""
    # TODO: could be implemented with nested_keys_to_dict/merge_dicts ?
    parts = path.split(".")
    try:
        kw = kwargs.copy()
        for part in parts[:-1]:
            kw2 = kw[part].copy()
            kw[part] = kw2
        default = kw[parts[-1]]
    except (KeyError, TypeError):
        default = None
    up = cls(default=default)
    kw[parts[-1]] = "{%s}" % name
    return kw, up


def _by_value(val, up, name):
    if isinstance(val, dict):
        return {k: _by_value(v, up, name) for k, v in val.items()}
    elif isinstance(val, Iterable) and not isinstance(val, (str, bytes)):
        return type(val)([_by_value(v, up, name) for v in val])
    elif isinstance(val, str) and isinstance(up.default, str) and up.default in val:
        return val.replace(up.default, "{%s}" % name)
    else:
        try:
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
