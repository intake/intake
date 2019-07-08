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
from ..config import confdir
from ..utils import yaml_load
logger = logging.getLogger('intake')


def autodiscover(path=None, plugin_prefix='intake_'):
    """Scan for intake drivers and return a dict of plugins.

    This wraps ``entrypoints.get_group_named('intake.drivers')``. In addition,
    for backward-compatibility, this also searches path (or sys.path) for
    packages with names that start with plugin_prefix.  Those modules will be
    imported and scanned for subclasses of intake.source.base.Plugin.  Any
    subclasses found will be instantiated and returned in a dictionary, with
    the plugin's name attribute as the key.
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

    group = entrypoints.get_group_all('intake.drivers')
    for entrypoint in group:
        # TODO Refer to intake configuration file to enable only a *subset* of
        # the installed extensions and/or to give precedence to a specific
        # driver in the event of a name collision.
        plugins[entrypoint.name] = entrypoint.load()
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
