import re
import warnings

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
from . import source
from .catalog.default import load_combo_catalog
from .catalog.base import Catalog, RemoteCatalog
from .catalog import local

__all__ = ['registry', 'make_open_functions', 'cat', 'Catalog']

from .source import registry
from .source.discovery import autodiscover

# Populate list of autodetected plugins
registry.update(autodiscover())

# FIXME: this plugin needs to eventually be retired
from .source import csv
registry['csv'] = csv.CSVSource
registry['empty'] = Catalog
registry['intake_remote'] = RemoteCatalog


def make_open_functions():
    """From the current state of ``registry``, create open_* functions"""
    # Create shortcut open methods
    if hasattr(str, 'isidentifier'):
        def isidentifier(x):
            return x.isidentifier()
    else:
        IDENTIFIER_REGEX = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*$')
        isidentifier = IDENTIFIER_REGEX.match
    for plugin_name, plugin in registry.items():
        func_name = 'open_' + plugin_name
        if not isidentifier(func_name):
            # primitive name normalization
            func_name = re.sub('[-=~^&|@+]', '_', func_name)
        if isidentifier(func_name):
            globals()[func_name] = plugin
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
cat = load_combo_catalog()


def open_catalog(*args, **kwargs):
    """LONG EXPLANATION HERE"""
    driver = kwargs.pop('driver', None)
    if driver is None:
        if args:
            uri = args[0]
            if ((isinstance(uri, str) and "*" in uri)
                    or ((isinstance(uri, (list, tuple))) and len(uri) > 1)):
                # glob string or list of files/globs
                driver = 'yaml_files_cat'
            elif isinstance(uri, (list, tuple)) and len(uri) == 1:
                args = (uri[0], ) + args[1:]
                if "*" in uri[0]:
                    # single glob string in a list
                    driver = 'yaml_files_cat'
                else:
                    # single filename in a list
                    driver = 'yaml_file_cat'
            elif isinstance(uri, str):
                # single URL
                if uri.startswith('intake:'):
                    # server
                    driver = 'intake_remote'
                else:
                    if uri.endswith(('.yml', '.yaml')):
                        driver = 'yaml_file_cat'
                    else:
                        args = (uri + '/*', ) + args[1:]
        else:
            # unknown - to error
            driver = 'empty'
    if driver not in registry:
        raise ValueError('Unknown catalog driver, supply one of: %s'
                         % list(registry))
    return registry[driver](*args, **kwargs)


Catalog = open_catalog
