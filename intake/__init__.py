# Plugin registry

from . import source
from ._version import get_versions


registry = source.registry

# Automatically create shortcut open methods
for key, value in registry.items():
    globals()['open_' + key] = value.open

__version__ = get_versions()['version']
del get_versions
