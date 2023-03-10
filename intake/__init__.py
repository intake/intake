# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import importlib
import logging
import os
import re
import warnings

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from .catalog.base import Catalog
from .source import registry

imports = {
    "DataSource": "intake.source.base:DataSource",
    "Schema": "intake.source.base:Schema",
    "load_combo_catalog": "intake.catalog.default:load_combo_catalog",
    "upload": "intake.container:upload",
    "gui": "intake.interface:instance",
    "interface": "intake.interface",
    "cat": "intake.catalog:builtin",
    "output_notebook": "intake.interface:output_notebook",
    "register_driver": "intake.source:register_driver",
    "unregister_driver": "intake.source:unregister_driver",
}
logger = logging.getLogger("intake")


def __getattr__(attr):
    """Lazy attribute propagator

    Defers inputs of functions until they are needed, according to the
    contents of the ``imports`` (submodules and classes) and ``openers``
    (functions which instantiate data sources directly)
    dicts. All keys in ``openers``
    must start with "open_", else they will be ignored.
    """
    gl = globals()

    if attr == "__all__":
        return __dir__()

    if attr in gl:
        return gl[attr]

    if attr in imports:
        dest = imports[attr]
        modname = dest.split(":", 1)[0]
        logger.debug("Importing: %s" % modname)
        mod = importlib.import_module(modname)
        if ":" in dest:
            mod = getattr(mod, dest.split(":")[1])
        gl[attr] = mod
        return mod

    if attr[:5] == "open_" and attr[5:] in registry.drivers.enabled_plugins():
        driver = registry[attr[5:]]  # "open_..."
        return driver

    raise AttributeError(attr)


def __dir__(*_, **__):
    openers = ["open_" + name for name in registry.drivers.enabled_plugins()]
    return sorted(list(globals()) + list(imports) + openers)


def open_catalog(uri=None, **kwargs):
    """Create a Catalog object

    Can load YAML catalog files, connect to an intake server, or create any
    arbitrary Catalog subclass instance. In the general case, the user should
    supply ``driver=`` with a value from the plugins registry which has a
    container type of catalog. File locations can generally be remote, if
    specifying a URL protocol.

    The default behaviour if not specifying the driver is as follows:

    - if ``uri`` is a single string ending in "yml" or "yaml", open it as a
      catalog file
    - if ``uri`` is a list of strings, a string containing a glob character
      ("*") or a string not ending in "y(a)ml", open as a set of catalog
      files. In the latter case, assume it is a directory.
    - if ``uri`` begins with protocol ``"intake:"``, connect to a remote
      Intake server
    - if ``uri`` is ``None`` or missing, create a base Catalog object without entries.

    Parameters
    ----------
    uri: str or pathlib.Path
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
    driver = kwargs.pop("driver", None)
    if isinstance(uri, os.PathLike):
        uri = os.fspath(uri)
    if driver is None:
        if uri:
            if (isinstance(uri, str) and "*" in uri) or ((isinstance(uri, (list, tuple))) and len(uri) > 1):
                # glob string or list of files/globs
                driver = "yaml_files_cat"
            elif isinstance(uri, (list, tuple)) and len(uri) == 1:
                uri = uri[0]
                if "*" in uri[0]:
                    # single glob string in a list
                    driver = "yaml_files_cat"
                else:
                    # single filename in a list
                    driver = "yaml_file_cat"
            elif isinstance(uri, str):
                # single URL
                if uri.startswith("intake:"):
                    # server
                    driver = "intake_remote"
                else:
                    if uri.endswith((".yml", ".yaml")):
                        driver = "yaml_file_cat"
                    else:
                        uri = uri.rstrip("/") + "/*.y*ml"
                        driver = "yaml_files_cat"
            else:
                raise ValueError("URI not understood: %s" % uri)
        else:
            # empty cat
            driver = "catalog"
    if "_file" not in driver:
        kwargs.pop("fs", None)
    if driver not in registry:
        raise ValueError("Unknown catalog driver (%s), supply one of: %s" % (driver, list(sorted(registry))))
    return registry[driver](uri, **kwargs)
