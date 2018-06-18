import re
import warnings

from . import source
from ._version import get_versions
from .catalog.default import load_combo_catalog
from .catalog import Catalog

__version__ = get_versions()['version']
del get_versions

__all__ = ['registry', 'Catalog', 'make_open_functions', 'cat']

registry = source.registry

# Create shortcut open methods
if hasattr(str, 'isidentifier'):
    def isidentifier(x):
        return x.isidentifier()
else:
    IDENTIFIER_REGEX = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*$')
    isidentifier = IDENTIFIER_REGEX.match


def make_open_functions():
    """From the current state of ``registry``, create open_* functions"""
    for plugin_name, plugin in registry.items():
        func_name = 'open_' + plugin_name
        if not isidentifier(func_name):
            # primitive name normalization
            func_name = re.sub('[-=~^&|@+]', '_', func_name)
        if isidentifier(func_name):
            globals()[func_name] = plugin.open
            __all__.append(func_name)
        else:
            warnings.warn('Invalid Intake plugin name "%s" found.' %
                          plugin_name)


def output_notebook(inline=True, logo=False):
    """
    Load the notebook extension

    Parameters
    ----------
    inline : boolean (optional)
        Whether to inline JS code or load it from a CDN
    logo : boolean (optional)
        Whether to show the logo(s)
    """
    try:
        import holoplot
        import holoviews as hv
    except:
        raise ImportError("The intake plotting API requires holoplot."
                          "holoplot may be installed with:\n\n"
                          "`conda install -c pyviz holoplot` or "
                          "`pip install holoplot`.")
    hv.extension('bokeh', inline=inline, logo=logo)


make_open_functions()

# Import default catalog
cat = load_combo_catalog()
