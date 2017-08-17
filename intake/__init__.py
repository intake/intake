# Plugin registry

from . import plugins
from .catalog import load_catalog

registry = plugins.registry

# Automatically create shortcut open methods
for key, value in registry.items():
    globals()['open_' + key] = value.open

try:
    from .dask import to_dask
except:
    pass
