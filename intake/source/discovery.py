#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import glob
import os
import pkgutil
import warnings
import importlib
import inspect
import time
import logging

import entrypoints
import yaml

from .base import DataSource
from ..catalog.base import Catalog
from ..config import load_conf, save_conf
from ..utils import yaml_load
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
        # TODO Refer to intake configuration file to enable only a *subset* of
        # the installed extensions and/or to give precedence to a specific
        # driver in the event of a name collision.
        try:
            drivers[entrypoint.name] = entrypoint.load()
        except ModuleNotFoundError:
            log.error(
                f"Failed to load {entrypoint.name} driver because module "
                f"{entrypoint.module_name} could not be found.")
            continue
        except ModuleNotFoundError:
            log.error(
                f"Failed to load {entrypoint.name} driver because no object "
                f"named {entrypoint.object_name} could be found in the module "
                f"{entrypoint.module_name}.")
            continue

    # TODO Un-comment this code to unleash warnings after most known intake-*
    # packages have been updated to provide 'intake.drivers' entrypoints.
    # missing_entry_points = set(package_scan_results) - set(drivers)
    # if missing_entry_points:
    #     warnings.warn(
    #         f"The drivers {missing_entry_points} do not specify entry_points "
    #         f"and were only discovered via a package scan. This may break in a "
    #         f"future release of intake. The packages should be updated.")
    drivers.update(package_scan_results)

    drivers_conf = (load_conf() or {}).get('drivers', {})
    for name, dotted_object_name in drivers_conf.items():
        if not dotted_object_name:
            # This driver is disabled.
            drivers.pop(name, None)
        module_name, object_name = dotted_object_name.rpartition('.')
        mock_entrypoint = entrypoint.EntryPoint(name, module_name, object_name)
        # If the user has a broken config and this fails, let this raise
        # instead of just logging.
        drivers[entrypoint.name] = entrypoint.load()
    return drivers

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


all_enabled_drivers = autodiscover


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


def enable(driver):
    drivers_d = os.path.join(confdir, 'drivers.d')
    os.makedirs(drivers_d, exist_ok=True)
    filepath = '{}.yml'.format(os.path.join(drivers_d, driver))
    if os.path.isfile(filepath):
        with open(filepath, 'r') as f:
            conf = yaml_load(f.read())
    else:
        conf = {}
    conf.update({driver: {'enabled': True}})
    with open(filepath, 'w') as f:
        f.write(yaml.dump(conf, default_flow_style=False))


def disable(driver):
    drivers_d = os.path.join(confdir, 'drivers.d')
    os.makedirs(drivers_d, exist_ok=True)
    filepath = '{}.yml'.format(os.path.join(drivers_d, driver))
    if os.path.isfile(filepath):
        with open(filepath, 'r') as f:
            conf = yaml_load(f.read())
    else:
        conf = {}
    conf.update({driver: {'enabled': False}})
    with open(filepath, 'w') as f:
        f.write(yaml.dump(conf, default_flow_style=False))
