import os
import re
from typing import Any, Iterable

import typeguard

from intake.readers.utils import Tokenizable


class BaseUserParameter(Tokenizable):
    """This base class is enough for simple type coercion."""

    def __init__(self, default, dtype=object):
        self.default = default
        self.dtype = dtype

    def set_default(self, value):
        value = self.coerce(value)
        if self.validate(value):
            self.default = value
        else:
            raise ValueError

    def coerce(self, value):
        if not isinstance(value, self.dtype):
            return self.dtype(value)
        return value

    def _validate(self, value):
        return self.coerce(value) is not None

    def validate(self, value) -> bool:
        try:
            return self._validate(value)
        except (TypeError, ValueError):
            return False


class OptionsUserParameter(BaseUserParameter):
    def __init__(self, default, options, dtype=object):
        super().__init__(default, dtype=dtype)
        self.options = options

    def _validate(self, value):
        return self.coerce(value) in self.options


class MultiOptionUserParameter(OptionsUserParameter):
    def __init__(self, default, options, dtype=list[Any]):
        super().__init__(default, options=options, dtype=dtype)

    def _validate(self, value):
        typeguard.check_type(value, self.dtype)
        return all(v in self.options for v in value)


class BoundedNumberUserParameter(BaseUserParameter):
    def __init__(self, default, dtype=float, max_value=None, min_value=None):
        super().__init__(default, dtype=dtype)
        self.max = max_value
        self.min = min_value

    def _validate(self, value):
        out = True
        if self.max:
            out = out and self.max > value
        if self.min:
            out = out and self.min < value
        return out


template_env = re.compile(r"[{]env[(]([^)]+)[)][}]")
template_subenv = re.compile(r"env[(]([^)]+)[)]")


def _set_values(up, arguments):
    if isinstance(arguments, dict):
        return {k: _set_values(up, v) for k, v in arguments.copy().items()}
    elif isinstance(arguments, str) and arguments.startswith("{") and arguments.endswith("}") and arguments[1:-1] in up:
        return up[arguments[1:-1]]
    elif isinstance(arguments, str):
        m = template_env.match(arguments)
        if m:
            var = m.groups()[0]
            return os.getenv(var)
        return arguments.format(**up)
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
            up[k].set_default(v)
            arguments.pop(k)
    for k, v in up.copy().items():
        if isinstance(v, str):
            m = template_subenv.match(v)
            if m:
                up[k] = v.set_default(m.groups()[0])
    return _set_values({k: u.default for k, u in up.items()}, arguments)
