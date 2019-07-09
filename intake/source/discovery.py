#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import pkgutil
import warnings
import importlib
import inspect
import time
import logging

import entrypoints

from .base import DataSource
from ..catalog.base import Catalog
from ..config import save_conf, conf
logger = logging.getLogger('intake')


def autodiscover(path=None, plugin_prefix='intake_', do_package_scan=True):
    """Discover intake drivers.

    In order of decreasing precedence:
    - Respect the 'drivers' section of the intake configuration file.
    - Find 'intake.drivers' entrypoints provided by any Python packages in the
      environment.
    - Search all packages in the environment for names that begin with
      ``intake_``. Import them and scan them for subclasses of
      ``intake.source.base.Plugin``. This was previously the *only* mechanism
      for auto-discoverying intake drivers, and it is maintained for backward
      compatibility. In a future release, intake will issue a warning if any
      packages are located by the method that do not also have entrypoints.

    Parameters
    ----------
    path : str or None
        Default is ``sys.path``.
    plugin_prefix : str
        DEPRECATED. Default is 'intake_'.
    do_package_scan : boolean
        Default is True.

    Returns
    -------
    drivers : dict
        Name mapped to driver class.
    """
    if do_package_scan:
        package_scan_results = _package_scan(path, plugin_prefix)

    drivers = {}
    group = entrypoints.get_group_all('intake.drivers', path=path)
    for entrypoint in group:
        try:
            drivers[entrypoint.name] = _load_entrypoint(entrypoint)
        except ConfigurationError:
            logger.exception()
            continue

    # TODO Un-comment this code to unleash warnings after most known intake_*
    # packages have been updated to provide 'intake.drivers' entrypoints.
    # missing_entry_points = set(package_scan_results) - set(drivers)
    # if missing_entry_points:
    #     warnings.warn(
    #         f"The drivers {missing_entry_points} do not specify entry_points "
    #         f"and were only discovered via a package scan. This may break in a "
    #         f"future release of intake. The packages should be updated.")
    for name, driver in package_scan_results.items():
        # In the event of a name collision, give the package scane results
        # lower precedence than entrypoints.
        drivers.setdefault(name, driver)

    drivers_conf = conf.get('drivers', {})
    for name, dotted_object_name in drivers_conf.items():
        if not dotted_object_name:
            logger.debug('Driver %s disabled in config file', name)
            # This driver is disabled.
            drivers.pop(name, None)
            continue
        module_name, object_name = dotted_object_name.rsplit('.', 1)
        entrypoint = entrypoints.EntryPoint(name, module_name, object_name)
        try:
            drivers[entrypoint.name] = _load_entrypoint(entrypoint)
        except ConfigurationError:
            logger.exception()
            continue
    return drivers


def _load_entrypoint(entrypoint):
    """
    Call entrypoint.load() and, if it fails, raise context-specific errors.
    """
    try:
        return entrypoint.load()
    except ModuleNotFoundError:
        raise ConfigurationError(
            f"Failed to load {entrypoint.name} driver because module "
            f"{entrypoint.module_name} could not be found.")
    except ModuleNotFoundError:
        raise ConfigurationError(
            f"Failed to load {entrypoint.name} driver because no object "
            f"named {entrypoint.object_name} could be found in the module "
            f"{entrypoint.module_name}.")


def _package_scan(path=None, plugin_prefix='intake_'):
    """Scan for intake drivers and return a dict of plugins.

    This searches path (or sys.path) for packages with names that start with
    plugin_prefix.  Those modules will be imported and scanned for subclasses
    of intake.source.base.Plugin.  Any subclasses found will be instantiated
    and returned in a dictionary, with the plugin's name attribute as the key.
    """

    plugins = {}

    for importer, name, ispkg in pkgutil.iter_modules(path=path):
        if name.startswith(plugin_prefix):
            t = time.time()
            new_plugins = load_plugins_from_module(name)

            for plugin_name, plugin in new_plugins.items():
                if plugin_name in plugins:
                    orig_path = inspect.getfile(plugins[plugin_name])
                    new_path = inspect.getfile(plugin)
                    warnings.warn('Plugin name collision for "%s" from'
                                  '\n    %s'
                                  '\nand'
                                  '\n    %s'
                                  '\nKeeping plugin from first location.'
                                  % (plugin_name, orig_path, new_path))
                else:
                    plugins[plugin_name] = plugin
            logger.debug("Import %s took: %7.2f s" % (name, time.time() - t))
    return plugins


def load_plugins_from_module(module_name):
    """Imports a module and returns dictionary of discovered Intake plugins.

    Plugin classes are instantiated and added to the dictionary, keyed by the
    name attribute of the plugin object.
    """
    plugins = {}

    try:
        if module_name.endswith('.py'):
            import imp
            mod = imp.load_source('module.name', module_name)
        else:
            mod = importlib.import_module(module_name)
    except Exception as e:
        logger.debug("Import module <{}> failed: {}".format(module_name, e))
        return {}
    for _, cls in inspect.getmembers(mod, inspect.isclass):
        # Don't try to register plugins imported into this module elsewhere
        if issubclass(cls, (Catalog, DataSource)):
            plugins[cls.name] = cls

    return plugins


class ConfigurationError(Exception):
    pass


def enable(name, driver):
    """
    Update config file drivers seciton to explicitly assign a driver to a name.

    Parameters
    ----------
    name : string
        As in ``'zarr'``
    driver : string
        Dotted object name, as in ``'intake_xarray.xzarr.ZarrSource'``
    """
    if 'drivers' not in conf:
        conf['drivers'] = {}
    conf['drivers'][name] = driver
    save_conf()


def disable(name):
    """Update config file drivers seciton to disable a name.

    Parameters
    ----------
    name : string
        As in ``'zarr'``
    """
    if 'drivers' not in conf:
        conf['drivers'] = {}
    conf['drivers'][name] = False
    save_conf()
