#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import re
import logging
import warnings

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
from . import source
from .source.base import Schema, DataSource
from .catalog.base import Catalog, RemoteCatalog
from .catalog import local
from .catalog.default import load_combo_catalog
from .container import upload
from .source import registry
from .source.discovery import autodiscover
from .gui import InstanceMaker

# Populate list of autodetected drivers (plugins).
registry.update(autodiscover())

from .source import csv, textfiles, npy, zarr
registry['csv'] = csv.CSVSource
registry['textfiles'] = textfiles.TextFilesSource
registry['catalog'] = Catalog
registry['intake_remote'] = RemoteCatalog
registry['numpy'] = npy.NPySource
registry['ndzarr'] = zarr.ZarrArraySource
logger = logging.getLogger('intake')


def make_open_functions():
    """From the current state of ``registry``, create open_* functions"""
    for plugin_name, plugin in registry.items():
        try:
            func_name = 'open_' + plugin_name
            if not func_name.isidentifier():
                # primitive name normalization
                func_name = re.sub('[-=~^&|@+]', '_', func_name)
            if func_name.isidentifier():
                globals()[func_name] = plugin
            else:
                warnings.warn('Invalid Intake plugin name "%s" found.' %
                              plugin_name)
        except:
            logger.warning("Creation of open function failed for %s"
                           "" % plugin_name)


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
        import hvplot
    except ImportError:
        raise ImportError("The intake plotting API requires hvplot."
                          "hvplot may be installed with:\n\n"
                          "`conda install -c pyviz hvplot` or "
                          "`pip install hvplot`.")
    import holoviews as hv
    return hv.extension('bokeh', inline=inline, logo=logo)


make_open_functions()
cat = load_combo_catalog()


def open_catalog(uri=None, **kwargs):
    """Create a Catalog object

    Can load YAML catalog files, connect to an intake server, or create any
    arbitrary Catalog subclass instance. In the general case, the user should
    supply ``driver=`` with a value from the plugins registry which has a
    container type of catalog. File locations can generally be remote, if
    specifying a URL protocol.

    The default behaviour if not specifying the driver is as follows:

    - if ``uri`` is a a single string ending in "yml" or "yaml", open it as a
      catalog file
    - if ``uri`` is a list of strings, a string containing a glob character
      ("*") or a string not ending in "y(a)ml", open as a set of catalog
      files. In the latter case, assume it is a directory.
    - if ``uri`` beings with protocol ``"intake:"``, connect to a remote
      Intake server
    - otherwise, create a base Catalog object without entries.

    Parameters
    ----------
    uri: str
        Designator for the location of the catalog.
    kwargs:
        passed to subclass instance, see documentation of the individual
        catalog classes. For example, ``yaml_files_cat`` (when specifying
        multiple uris or a glob string) takes the additional
        parameter ``flatten=True|False``, specifying whether all data sources
        are merged in a single namespace, or each file becomes
        a sub-catalog.

    See also
    --------
    intake.open_yaml_files_cat, intake.open_yaml_file_cat,
    intake.open_intake_remote
    """
    driver = kwargs.pop('driver', None)
    if driver is None:
        if uri:
            if ((isinstance(uri, str) and "*" in uri)
                    or ((isinstance(uri, (list, tuple))) and len(uri) > 1)):
                # glob string or list of files/globs
                driver = 'yaml_files_cat'
            elif isinstance(uri, (list, tuple)) and len(uri) == 1:
                uri = uri[0]
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
                        uri = uri.rstrip('/') + '/*.y*ml'
                        driver = 'yaml_files_cat'
        else:
            # empty cat
            driver = 'catalog'
    if '_file' not in driver:
        kwargs.pop('fs', None)
    if driver not in registry:
        raise ValueError('Unknown catalog driver (%s), supply one of: %s'
                         % (driver, list(sorted(registry))))
    return registry[driver](uri, **kwargs)


Catalog = open_catalog
gui = InstanceMaker()
