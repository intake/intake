import re
from hashlib import md5
from typing import Iterable


def subclasses(cls):
    out = set()
    for cl in cls.__subclasses__():
        out.add(cl)
        out |= subclasses(cl)
    return out


def merge_dicts(*dicts):
    out = {}
    for dic in dicts:
        for k, v in dic.items():
            out[k] = v
    return out


func_or_method = re.compile(r"<(function|method) ([^ ]+) at 0x[0-9a-f]+>")


def _func_to_str(f):
    return "{func(%s)}" % f"{f.__module__}:{f.__name__}"


def find_funcs(val):
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
    _tok = None

    @property
    def token(self):
        # TODO: this effectively says that mutation, if allowed, does not change token
        #  implyng that only _ attributes are multable, such as _metadata
        if self._tok is None:
            # TODO: walk dict and use tokens of instances of Tokenizable therein?
            dic = {k: find_funcs for k, v in self.__dict__.items() if not k.startswith("_")}
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


def make_cls(cls, kwargs):
    return cls(**kwargs)


def extract_by_path(path, cls, name, kwargs):
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


def extract_by_value(value, cls, name, kwargs):
    up = cls(default=value)
    kw = _by_value(kwargs, up, name)
    return kw, up
