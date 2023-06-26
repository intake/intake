from typing import Any

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
