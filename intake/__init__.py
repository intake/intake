import warnings

from . import source
from ._version import get_versions


__all__ = ['registry']

registry = source.registry

# Create shortcut open methods
if hasattr(str, 'isidentifier'):
    def isidentifier(x):
        return x.isidentifier()
else:
    import re
    IDENTIFIER_REGEX = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*$')
    isidentifier = IDENTIFIER_REGEX.match

for plugin_name, plugin in registry.items():
    func_name = 'open_' + plugin_name
    if isidentifier(func_name):
        globals()[func_name] = plugin.open
        __all__.append(func_name)
    else:
        warnings.warn('Invalid Intake plugin name "%s" found.' % plugin_name)


__version__ = get_versions()['version']
del get_versions
