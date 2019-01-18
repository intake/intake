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

import six
import yaml

from .base import DataSource
from ..catalog.base import Catalog
from ..config import confdir
logger = logging.getLogger('intake')


def autodiscover(path=None, plugin_prefix='intake_'):
    """Scan for Intake plugin packages and return a dict of plugins.

    This function searches path (or sys.path) for packages with names that
    start with plugin_prefix.  Those modules will be imported and scanned for
    subclasses of intake.source.base.Plugin.  Any subclasses found will be
    instantiated and returned in a dictionary, with the plugin's name attribute
    as the key.
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


def autodiscover_with_duplicates(path=None, plugin_prefix='intake_'):
    """Scan for Intake plugin packages and return a list of plugins.

    Unlike autodiscover, this will include name collisions.

    This function searches path (or sys.path) for packages with names that
    start with plugin_prefix.  Those modules will be imported and scanned for
    subclasses of intake.source.base.Plugin.  Any subclasses found will be
    instantiated and returned in a dictionary, with the plugin's name attribute
    as the key.
    """

    plugins = []

    for importer, name, ispkg in pkgutil.iter_modules(path=path):
        if name.startswith(plugin_prefix):
            t = time.time()
            new_plugins = load_plugins_from_module(name)

            for plugin_name, plugin in new_plugins.items():
                plugins.append((plugin_name, plugin))
            logger.debug("Import %s took: %7.2f s" % (name, time.time() - t))

    return plugins


def all_enabled_drivers():
    """
    Return all enabled drivers as mapping of names to classes.

    Find drivers (plugins) that are autodiscoverable (i.e. start with 'intake')
    and those that are not autodiscovered but have enabled configuration in
    drivers.d. Do not exclude any plugs that have explicitly disable
    configuration, regardless of autodiscoverability.
    """
    drivers = {}
    autodiscovered = autodiscover_with_duplicates()
    # Add each autodiscovered driver after checking that it is not disabled.
    # In the event of a name collision, give precedence to the first one found.
    drivers_d = os.path.join(confdir, 'drivers.d')
    for name, cls in autodiscovered:
        filename = '{cls.__module__}.{cls.__name__}.yml'.format(cls=cls)
        filepath = os.path.join(drivers_d, filename)
        if os.path.isfile(filepath):
            try:
                driver = enabled_driver(filepath)
            except ConfigurationError:
                logger.exception("Error reading %s", filepath)
            else:
                if driver and name not in drivers:
                    drivers[name] = cls
        else:
            # This is not explicitly enabled or disabled. Enable it by default.
            if name not in drivers:
                drivers[name] = cls
    # Now add drivers that are not autodiscoverable (i.e. do not begin with
    # 'intake') but do have enabled configuration as drivers.d.
    # If these collide with any autodiscovered drivers, the explicitly
    # enabled ones take precedence.
    for filepath in glob.glob(os.path.join(drivers_d, '*.yml')):
        try:
            driver = enabled_driver(filepath)
        except ConfigurationError:
            logger.exception("Error reading %s", filepath)
            continue
        if driver:
            drivers[driver.name] = driver
    return drivers


def enabled_driver(filepath):
    """
    From a driver.d config, load driver class if its configuration is enabled.
    """
    with open(filepath) as f:
        try:
            conf = yaml.load(f.read())
        except Exception as err:
            six.raise_from(
                ConfigurationError("Could not parse {} as YAML."
                .format(filepath)), err)
    try:
        # Conf looks like:
        # {'some.module.path.ClassName': {'enabled': <boolean>}}
        (module_and_class, val), = conf.items()
        enabled = val['enabled']
    except KeyError as err:
        six.raise_from(
            ConfigurationError("Could not find expected structure in "
                               "{}".format(filepath)), err)
    if not enabled:
        return False
    pieces = module_and_class.split('.')
    module_name = '.'.join(pieces[:-1])
    class_name = pieces[-1]
    mod = importlib.import_module(module_name)
    cls = getattr(mod, class_name)
    return cls


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
