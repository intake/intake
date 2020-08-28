#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import collections
from collections import OrderedDict
import collections.abc
import datetime
from contextlib import contextmanager
import warnings
import yaml
import sys


def make_path_posix(path):
    """ Make path generic """
    if '://' in path:
        return path
    return path.replace('\\', '/').replace('//', '/')


def no_duplicates_constructor(loader, node, deep=False):
    """Check for duplicate keys while loading YAML

    https://gist.github.com/pypt/94d747fe5180851196eb
    """

    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping:
            from intake.catalog.exceptions import DuplicateKeyError

            raise DuplicateKeyError("while constructing a mapping",
                                    node.start_mark,
                                    "found duplicate key (%s)" % key,
                                    key_node.start_mark)
        mapping[key] = value

    return loader.construct_mapping(node, deep)


def tuple_constructor(loader, node, deep=False):
    return tuple(loader.construct_object(node, deep=deep)
                 for node in node.value)


def represent_dictionary_order(self, dict_data):
    return self.represent_mapping('tag:yaml.org,2002:map', dict_data.items())


yaml.add_representer(OrderedDict, represent_dictionary_order)


@contextmanager
def no_duplicate_yaml():
    yaml.SafeLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        no_duplicates_constructor)
    yaml.SafeLoader.add_constructor('tag:yaml.org,2002:python/tuple',
                                    tuple_constructor)
    try:
        yield
    finally:
        yaml.SafeLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            yaml.constructor.SafeConstructor.construct_yaml_map
        )


def yaml_load(stream):
    """Parse YAML in a context where duplicate keys raise exception"""
    with no_duplicate_yaml():
        return yaml.safe_load(stream)


def classname(ob):
    """Get the object's class's name as package.module.Class"""
    import inspect
    if inspect.isclass(ob):
        return '.'.join([ob.__module__, ob.__name__])
    else:
        return '.'.join([ob.__class__.__module__, ob.__class__.__name__])


class DictSerialiseMixin(object):

    __tok_cache = None

    def __new__(cls, *args, **kwargs):
        """Capture creation args when instantiating"""
        o = object.__new__(cls)
        o._captured_init_args = args
        o._captured_init_kwargs = kwargs

        return o

    @property
    def classname(self):
        return classname(self)

    def __dask_tokenize__(self):
        if self.__tok_cache is None:
            from dask.base import tokenize
            self.__tok_cache = tokenize(self.__getstate__())
        return self.__tok_cache

    def __getstate__(self):
        args = [arg.__getstate__() if isinstance(arg, DictSerialiseMixin)
                else arg
                for arg in self._captured_init_args]
        # We employ OrderedDict in several places. The motivation
        # is to speed up dask tokenization. When dask tokenizes a plain dict,
        # it sorts the keys, and it turns out that this sort operation
        # dominates the call time, even for very small dicts. Using an
        # OrderedDict steers dask toward a different and faster tokenization.
        kwargs = collections.OrderedDict({
            k: arg.__getstate__()
            if isinstance(arg, DictSerialiseMixin) else arg
            for k, arg in self._captured_init_kwargs.items()
        })
        return collections.OrderedDict(cls=self.classname,
                                       args=args,
                                       kwargs=kwargs)

    def __setstate__(self, state):
        # reconstitute instances here
        self._captured_init_kwargs = state['kwargs']
        self._captured_init_args = state['args']
        state.pop('cls', None)
        self.__init__(*state['args'], **state['kwargs'])

    def __hash__(self):
        from dask.base import tokenize
        return int(tokenize(self), 16)

    def __eq__(self, other):
        return hash(self) == hash(other)


def remake_instance(data):
    import importlib
    if isinstance(data, str):
        data = {'cls': data}
    else:
        data = data.copy()
    mod, klass = data.pop('cls').rsplit('.', 1)
    module = importlib.import_module(mod)
    cl = getattr(module, klass)
    return cl(*data.get('args', ()), **data.get('kwargs', {}))


def pretty_describe(object, nestedness=0, indent=2):
    """Maintain dict ordering - but make string version prettier"""
    if not isinstance(object, dict):
        return str(object)
    sep = f'\n{" " * nestedness * indent}'
    out = sep.join((f'{k}: {pretty_describe(v, nestedness + 1)}' for k, v in object.items()))
    if nestedness > 0 and out:
        return f'{sep}{out}'
    return out


def decode_datetime(obj):
    import numpy
    if not isinstance(obj, numpy.ndarray) and "__datetime__" in obj:
        try:
            obj = datetime.datetime.strptime(
                    obj["as_str"],
                    "%Y%m%dT%H:%M:%S.%f%z",
            )
        except ValueError: # Perhaps lacking tz info
            obj = datetime.datetime.strptime(
                    obj["as_str"],
                    "%Y%m%dT%H:%M:%S.%f",
            )
    return obj


def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return {"__datetime__": True, "as_str": obj.strftime("%Y%m%dT%H:%M:%S.%f%z")}
    return obj


class RegistryView(collections.abc.Mapping):
    """
    Wrap registry dict in a read-only dict view.

    Subclasses define attributes filled into warning and error messages:
    - self._registry_name
    - self._register_func_name
    - self._unregister_func_name
    """
    def __init__(self, registry):
        self._registry = registry

    def __repr__(self):
        return f"{self.__class__.__name__}({self._registry!r})"

    def __getitem__(self, key):
        return self._registry[key]

    def __iter__(self):
        yield from self._registry

    def __len__(self):
        return len(self._registry)

    # Support the common mutation methods for now, but warn.

    def update(self, *args, **kwargs):
        warnings.warn(
            f"In a future release of intake, the {self._registry_name} will "
            f"not be directly mutable. Use {self._register_func_name}.",
            DeprecationWarning)
        self._registry.update(*args, **kwargs)
        # raise TypeError(
        #     f"The registry cannot be edited directly. "
        #     f"Instead, use the {self._register_func_name{")

    def __setitem__(self, key, value):
        warnings.warn(
            f"In a future release of intake, the {self._registry_name} will "
            f"not be directly mutable. Use {self._register_func_name}.",
            DeprecationWarning)
        self._registry[key] = value
        # raise TypeError(
        #     f"The registry cannot be edited directly. "
        #     f"Instead, use the {self._register_func_name{")

    def __delitem__(self, key):
        warnings.warn(
            f"In a future release of intake, the {self._registry_name} will "
            f"not be directly mutable. Use {self._unregister_func_name}.",
            DeprecationWarning)
        del self._registry[key]
        # raise TypeError(
        #     f"The registry cannot be edited directly. "
        #     f"Instead, use the {self._unregister_func_name{")


class DriverRegistryView(RegistryView):
    # This attributes are used by the base class
    # to fill in warning and error messages.
    _registry_name = "intake.registry"
    _register_func_name = "intake.register_driver"
    _unregister_func_name = "intake.unregister_driver"


class ContainerRegistryView(RegistryView):
    # This attributes are used by the base class
    # to fill in warning and error messages.
    _registry_name = "intake.container_map"
    _register_func_name = "intake.register_container"
    _unregister_func_name = "intake.unregister_container"


class ModuleImporter:
    def __init__(self, destination):
        self.destination = destination
        self.module = None

    def __getattribute__(self, item):
        d = object.__getattribute__(self, "__dict__")
        if item in d:
            return d[item]
        if self.module is None:
            print("Importing module: ", self.destination)
            self.module = __import__(self.destination)
        else:
            print("Referencing module: ", self.destination)
        sys.modules[self.destination] = self.module
        return getattr(self.module, item)
