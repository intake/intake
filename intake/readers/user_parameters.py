import builtins
import os
import re
from typing import Any, Iterable

from intake import import_name
from intake.readers.utils import Tokenizable


class BaseUserParameter(Tokenizable):
    """The base class allows for any default without checking/coercing"""

    fields = {"_description"}

    def __init__(self, default, description=""):
        self.default = default
        self._description = description

    def __repr__(self):
        dic = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return f"{type(self).__name__}, {self._description}\n{dic}"

    def set_default(self, value):
        value = self.coerce(value)
        if self.validate(value):
            self.default = value
        else:
            raise ValueError

    def with_default(self, value):
        import copy

        up = copy.copy(self)
        up.set_default(value)
        return up

    def coerce(self, value):
        return value

    def _validate(self, value):
        return True

    def validate(self, value) -> bool:
        try:
            return self._validate(value)
        except (TypeError, ValueError):
            return False


class SimpleUserParameter(BaseUserParameter):
    """This class is enough for simple type coercion."""

    def __init__(self, dtype: type = object, **kw):
        self.dtype = dtype.__name__
        if self.dtype not in dir(builtins) or not isinstance(dtype, type):
            raise ValueError("Only supports classes from the builtins module")
        super().__init__(**kw)

    @property
    def _dtype(self):
        return getattr(builtins, self.dtype)

    def coerce(self, value):
        if not isinstance(value, self._dtype):
            return self._dtype(value)  # works for dtype like str, int, list
        return value

    def _validate(self, value):
        return isinstance(value, self._dtype)


class OptionsUserParameter(SimpleUserParameter):
    """One choice out of a given allow list"""

    def __init__(self, options, dtype=object, **kw):
        super().__init__(dtype=dtype, **kw)
        self.options = {self.coerce(o) for o in options}

    def _validate(self, value):
        return self.coerce(value) in self.options


class MultiOptionUserParameter(OptionsUserParameter):
    """Multiple choices out of a given allow list/tuple"""

    def __init__(self, options: list | tuple, dtype=object, **kw):
        super().__init__(options=options, dtype=dtype, **kw)

    def coerce_one(self, value):
        return super().coerce(value)

    def coerce(self, value):
        return [self.coerce_one(v) for v in value]

    def _validate(self, value):
        return isinstance(value, (list, tuple)) and all(v in self.options for v in value)


class BoundedNumberUserParameter(SimpleUserParameter):
    """A number within a range bound"""

    def __init__(self, dtype=float, max_value=None, min_value=None, **kw):
        super().__init__(dtype=dtype, **kw)
        self.max = max_value
        self.min = min_value

    def _validate(self, value):
        out = True
        if self.max:
            out = out and self.max > value
        if self.min:
            out = out and self.min < value
        return out


template_env = re.compile(r"env[(]([^)]+)[)]")
template_data = re.compile(r"data[(]([^)]+)[)]")
template_func = re.compile(r"func[(]([^)]+)[)]")


def _set_values(up, arguments):
    from intake.readers.entry import ReaderDescription

    if isinstance(arguments, dict):
        return {k: _set_values(up, v) for k, v in arguments.copy().items()}
    elif isinstance(arguments, str) and arguments.startswith("{") and arguments.endswith("}"):
        arg = arguments[1:-1]
        if arg in up:
            return up[arguments[1:-1]]
        else:
            m = template_env.match(arg)
            if m:
                var = m.groups()[0]
                return os.getenv(var)
            m = template_data.match(arg)
            if m:
                var = m.groups()[0]
                thing = up[var]
                if isinstance(thing, ReaderDescription):
                    thing = thing.to_reader(user_parameters=up)
                return thing
            m = template_func.match(arg)
            if m:
                var = m.groups()[0]
                return import_name(var)

    if isinstance(arguments, str):
        envdict = {f"env({k})": os.getenv(k) for k in template_env.findall(arguments)}
        return arguments.format(**up, **envdict)
    elif isinstance(arguments, Iterable):
        return type(arguments)([_set_values(up, v) for v in arguments])
    return arguments


def set_values(user_parameters: dict[str, BaseUserParameter], arguments: dict[str, Any]):
    """Walk kwargs and set the value and types of any parameters found

    If one of arguments matches the name of a user_parameter, it will set the value of that
    parameter before proceeding.
    """
    up = user_parameters.copy()
    for k, v in arguments.copy().items():
        if k in user_parameters:
            up[k] = up[k].with_default(v)
            arguments.pop(k)
    for k, v in up.copy().items():
        # v can be a literal DataDescription (from a reader) rather than a UP
        if isinstance(getattr(v, "default", None), str):
            m = template_env.match(v.default)
            if m:
                up[k] = up[k].with_default(os.getenv(m.groups()[0]))
            m = template_func.match(v.default)
            if m:
                var = m.groups()[0]
                up[k] = up[k].with_default(import_name(var))

    return _set_values({k: (u.default if isinstance(u, BaseUserParameter) else u) for k, u in up.items()}, arguments)
