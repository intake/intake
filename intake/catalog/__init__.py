import importlib
from .base import Catalog

_registry = {
    'base': Catalog
}


def register(name, klass):
    if isinstance(klass, str):
        mod, label = str.rstrip('.', 1)
        mod = importlib.import_module()
        klass = getattr(mod, label)
    _registry[name] = klass


def get_catalog_class(name):
    return _registry.get(name, Catalog)
