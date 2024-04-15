"""
Parametrization of data/reader entries, as they appear in Catalogs

Parameters can be used to template values across readers, wither to indicate
choices that a user wishes to make at run time, or to require the user to fill
in details such as credentials. Providing options in this way is a simpler
experience for a user than replicating a reader/pipeline with different options.

In a catalog, user parameters can exist at the global scope, to be used anywhere,
as a part of the data description, to be inherited by all readers of that data,
or as part of the reader-specific descriptions. The value can be set in-place,
or provided during ``read()``.
"""
from __future__ import annotations

import builtins
import os
import re
from typing import Any, Iterable

from intake import import_name, conf
from intake.readers.utils import FormatWithPassthrough, SecurityError, Tokenizable


class BaseUserParameter(Tokenizable):
    """The base class allows for any default without checking/coercing"""

    def __init__(self, default, description=""):
        self.default = default  #: the value to use without user input
        self.description = description  #: what is the function of this parameter

    def __repr__(self):
        dic = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return f"{type(self).__name__}, {self.description}\n{dic}"

    def set_default(self, value):
        """Change the default, if it validates"""
        value = self.coerce(value)
        if self.validate(value):
            self.default = value
        else:
            raise ValueError("Could not validate %s with %s", value, self)

    def with_default(self, value):
        """A new instance with different default, if it validates

        (original object is left unchanged)
        """
        import copy

        up = copy.copy(self)
        up.set_default(value)
        return up

    def coerce(self, value):
        """Change given type to one that matches this parameter's intent"""
        return value

    def _validate(self, value):
        return True

    def validate(self, value) -> bool:
        """Is the given value allowed by this parameter?

        Exceptions are treated as False
        """
        try:
            return self._validate(value)
        except (TypeError, ValueError):
            return False

    def to_dict(self):
        dic = super().to_dict()
        dic["cls"] = self.qname()
        return dic


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


# TODO: Date type and generic functions of user_parameters like date(value).day

templates = {}


class NoMatch(ValueError):
    ...


def register_template(name):
    def wrapper(func):
        regex = re.compile(f"{name}[(]([^)]+)[)]")
        templates[regex] = func

        def go(text):
            m = regex.match(text)
            if m:
                return func(m)
            raise NoMatch

        return go

    return wrapper


template_env = re.compile(r"env[(]([^)]+)[)]")
template_data = re.compile(r"data[(]([^)]+)[)]")
template_func = re.compile(r"func[(]([^)]+)[)]")


@register_template(r"env")
def env(match, up):
    """Value from an environment variable"""
    return os.getenv(match.groups()[0])


@register_template(r"data")
def data(match, up):
    """The value from reading a dataset

    Used in pipelines to point to the outputs of upstream readers
    """
    # TODO: this might never be called, since Catalog._rehydrate does this job
    from intake.readers.convert import Pipeline
    from intake.readers.entry import ReaderDescription

    var = match.groups()[0]
    if "," in var:
        var, part = var.split(",")
    else:
        part = None
    thing = up[var.strip()]
    if isinstance(thing, ReaderDescription):
        thing = thing.to_reader(user_parameters=up)
        if part and isinstance(thing, Pipeline):
            thing = thing.first_n_stages(int(part))

    return thing


@register_template(r"import")
@register_template(r"func")
def imp(match, up):
    """The result of importing the string, an arbitrary python object

    Format of the input string is like "{import(package.module:object)}"
    """
    if not conf["allow_import"]:
        from intake.readers.utils import SecurityError

        raise SecurityError("Arbitrary imports are not allowed by the Intake config")
    return import_name(match.groups()[0])


@register_template(r"pickle64")
def unpickle(match, up):
    if not conf["allow_pickle"]:
        raise SecurityError("Unpickling is disallowed by the Intake config")
    import base64
    import pickle

    return pickle.loads(base64.b64decode(match.groups()[0].encode()))


def _set_values(up, arguments):
    if isinstance(arguments, dict):
        return {k: _set_values(up, v) for k, v in arguments.copy().items()}
    elif isinstance(arguments, str) and arguments.startswith("{") and arguments.endswith("}"):
        arg = arguments[1:-1]
        if arg in up:
            return up[arguments[1:-1]]
        else:
            for k, v in templates.items():
                m = re.match(k, arg)
                if m:
                    return v(m, up)
    if isinstance(arguments, str):
        # missing env keys become empty strings
        envdict = {f"env({k})": os.getenv(k) for k in template_env.findall(arguments)}
        data = FormatWithPassthrough(**up)
        data.update(envdict)
        try:
            out = arguments.format_map(data)  # missing keys remain unformatted, but don't raise
        except ValueError:
            # in case this is a string with genuine "{"s
            out = arguments
        return out
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
            u = up[k]
            if isinstance(u, BaseUserParameter):
                up[k] = up[k].with_default(v)
            else:
                up[k] = v
            arguments.pop(k)
    for k, v in up.copy().items():
        # v can be a literal DataDescription (from a reader) rather than a UP
        if isinstance(getattr(v, "default", None), str):
            for templ, func in templates.items():
                m = re.match(templ, v.default)
                if m:
                    up[k] = up[k].with_default(func(m, up))
            # m = template_env.match(v.default)
            # if m:
            #     up[k] = up[k].with_default(os.getenv(m.groups()[0]))
            # m = template_func.match(v.default)
            # if m:
            #     var = m.groups()[0]
            #     up[k] = up[k].with_default(import_name(var))

    return _set_values(
        {k: (u.default if isinstance(u, BaseUserParameter) else u) for k, u in up.items()},
        arguments,
    )
