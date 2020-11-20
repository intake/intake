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
import itertools
import time
import logging

import entrypoints

from ..config import save_conf, conf, cfile
logger = logging.getLogger('intake')


def autodiscover(path=None, plugin_prefix='intake_', do_package_scan=False):
    r"""Discover intake drivers.

    In order of decreasing precedence:

    - Respect the 'drivers' section of the intake configuration file.
    - Find 'intake.drivers' entrypoints provided by any Python packages in the
      environment.
    - Search all packages in the environment for names that begin with
      ``intake\_``. Import them and scan them for subclasses of
      ``intake.source.base.DataSourceBase``. This was previously the *only* mechanism
      for auto-discoverying intake drivers, and it is maintained for backward
      compatibility.

    Parameters
    ----------
    path : str or None
        Default is ``sys.path``.
    plugin_prefix : str
        DEPRECATED. Default is 'intake\_'.
    do_package_scan : boolean
        Whether to look for intake source classes in packages named
        "intake_*". This has been superceded by entrypoints declarations.

    Returns
    -------
    drivers : dict
        Name mapped to driver class.
    """
    # Discover drivers via package scan.
    if do_package_scan:
        package_scan_results = _package_scan(path, plugin_prefix)
        if package_scan_results:
            warnings.warn(
                "The option `do_package_scan` may be removed in a future release.",
                PendingDeprecationWarning)
    else:
        package_scan_results = {}

    # Discover drivers via entrypoints.
    group = entrypoints.get_group_named('intake.drivers', path=path)
    group_all = entrypoints.get_group_all('intake.drivers', path=path)
    if len(group_all) != len(group):
        # There are some name collisions. Let's go digging for them.
        for name, matches in itertools.groupby(group_all, lambda ep: ep.name):
            matches = list(matches)
            if len(matches) != 1:
                winner = group[name]
                logger.debug(
                    "There are %d 'intake.driver' entrypoints for the name "
                    "%r. They are %r. The match %r has won the race.",
                    len(matches),
                    name,
                    matches,
                    winner)

    for name, entrypoint in group.items():
        logger.debug("Discovered entrypoint '%s = %s.%s'",
                     name,
                     entrypoint.module_name,
                     entrypoint.object_name)
        if name in package_scan_results:
            cls = package_scan_results[name]
            del package_scan_results[name]
            logger.debug("Entrypoint shadowed package_scan result '%s = %s.%s'",
                         name, cls.__module__, cls.__name__)

    # Discover drivers via config.
    drivers_conf = conf.get('drivers', {})
    logger.debug("Using configuration file at %s", cfile())
    for name, dotted_object_name in drivers_conf.items():
        if not dotted_object_name:
            logger.debug('Name %s is banned in config file', name)
            if name in group:
                entrypoint = group[name]
                del group[name]
                logger.debug("Disabled entrypoint '%s = %s.%s'",
                             entrypoint.name,
                             entrypoint.module_name,
                             entrypoint.object_name)
            if name in package_scan_results:
                cls = package_scan_results[name]
                del package_scan_results[name]
                logger.debug("Disabled package_scan result '%s = %s.%s'",
                             name, cls.__module__, cls.__name__)
            continue
        module_name, object_name = dotted_object_name.rsplit('.', 1)
        entrypoint = entrypoints.EntryPoint(name, module_name, object_name)
        logger.debug("Discovered config-specified '%s = %s.%s'",
                     entrypoint.name,
                     entrypoint.module_name,
                     entrypoint.object_name)
        if name in group:
            shadowed = group[name]
            logger.debug("Config shadowed entrypoint '%s = %s.%s'",
                         shadowed.name,
                         shadowed.module_name,
                         shadowed.object_name)
        if name in package_scan_results:
            cls = package_scan_results[name]
            del package_scan_results[name]
            logger.debug("Config shadowed package scan result '%s = %s.%s'",
                         name, cls.__module__, cls.__name__)
        group[name] = entrypoint

    # Discovery is complete.

    if package_scan_results:
        warnings.warn(
            f"The drivers {list(package_scan_results)} do not specify entry_"
            f"points and were only discovered via a package scan. This may "
            f"break in a future release of intake. The packages should be "
            f"updated.",
            FutureWarning)

    # Load entrypoints. Any that were shadowed or banned have already been
    # removed above.
    drivers = {}
    for entrypoint in group.values():
        try:
            drivers[entrypoint.name] = _load_entrypoint(entrypoint)
        except ConfigurationError:
            logger.exception(
                "Error while loading entrypoint %s",
                entrypoint.name)
            continue
        logger.debug("Loaded entrypoint '%s = %s.%s'",
                     entrypoint.name,
                     entrypoint.module_name,
                     entrypoint.object_name)

    # Now include any package scan results. Any that were shadowed or
    # banned have already been removed above.
    for name, cls in package_scan_results.items():
        drivers[name] = cls
        logger.debug("Loaded package scan result '%s = %s.%s'",
                     name,
                     cls.__module__,
                     cls.__name__)

    return drivers


def autodiscover_all(path=None, plugin_prefix='intake_', do_package_scan=True):
    """Discover intake drivers including those registered for the same name.

    Parameters
    ----------
    path : str or None
        Default is ``sys.path``.
    plugin_prefix : str
        DEPRECATED. Default is 'intake_'.
    do_package_scan : boolean
        Default is True. In the future, the default will be changed to False,
        and the option may eventually be removed entirely.

    Returns
    -------
    drivers : list
        Each entry is a tuple: ``(name, driver_class)``.
    """
    # Discover drivers via package scan.
    if do_package_scan:
        warnings.warn(
            "The option `do_package_scan` may be removed in a future release.",
            PendingDeprecationWarning)
        package_scan_results = _package_scan(path, plugin_prefix)
    else:
        package_scan_results = {}

    # Discover drivers via entrypoints.
    group_all = entrypoints.get_group_all('intake.drivers', path=path)
    for entrypoint in group_all:
        logger.debug("Discovered entrypoint '%s = %s.%s'",
                     entrypoint.name,
                     entrypoint.module_name,
                     entrypoint.object_name)

    # Discover drivers via config.
    drivers_conf = conf.get('drivers', {})
    logger.debug("Using configuration file at %s", cfile())
    for name, dotted_object_name in drivers_conf.items():
        if not dotted_object_name:
            continue
        module_name, object_name = dotted_object_name.rsplit('.', 1)
        entrypoint = entrypoints.EntryPoint(name, module_name, object_name)
        logger.debug("Discovered config-specified '%s = %s.%s'",
                     entrypoint.name,
                     entrypoint.module_name,
                     entrypoint.object_name)
        group_all.append(entrypoint)

    # Load entrypoints. Any that were shadowed or banned have already been
    # removed above.
    drivers = []
    for entrypoint in group_all:
        try:
            drivers.append((entrypoint.name, _load_entrypoint(entrypoint)))
        except ConfigurationError:
            logger.exception(
                "Error while loading entrypoint %s",
                entrypoint.name)
            continue
        logger.debug("Loaded entrypoint '%s = %s.%s'",
                     entrypoint.name,
                     entrypoint.module_name,
                     entrypoint.object_name)

    # Now include any package scan results. Any that were shadowed or
    # banned have already been removed above.
    for name, cls in package_scan_results.items():
        drivers.append((name, cls))
        logger.debug("Loaded package scan result '%s = %s.%s'",
                     name,
                     cls.__module__,
                     cls.__name__)

    return drivers


def _load_entrypoint(entrypoint):
    """
    Call entrypoint.load() and, if it fails, raise context-specific errors.
    """
    try:
        return entrypoint.load()
    except ModuleNotFoundError as err:
        raise ConfigurationError(
            f"Failed to load {entrypoint.name} driver because module "
            f"{entrypoint.module_name} could not be found.") from err
    except AttributeError as err:
        raise ConfigurationError(
            f"Failed to load {entrypoint.name} driver because no object "
            f"named {entrypoint.object_name} could be found in the module "
            f"{entrypoint.module_name}.") from err


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
    from intake.source import DataSource
    from intake.catalog import Catalog
    plugins = {}

    try:
        try:
            mod = importlib.import_module(module_name)
        except ImportError as error:
            if module_name.endswith('.py'):
                # Provide a specific error regarding the removal of behavior
                # that intake formerly supported.
                raise ImportError(
                    "Intake formerly supported executing arbitrary Python "
                    "files not on the sys.path. This is no longer supported. "
                    "Drivers must be specific with a module path like "
                    "'package_name.module_name, not a Python filename like "
                    "'module.py'.") from error
            else:
                raise
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
    Update config file drivers section to explicitly assign a driver to a name.

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
    """Update config file drivers section to disable a name.

    Parameters
    ----------
    name : string
        As in ``'zarr'``
    """
    if 'drivers' not in conf:
        conf['drivers'] = {}
    conf['drivers'][name] = False
    save_conf()


def register_all():
    from intake.source import register_driver
    for name, driver in autodiscover().items():
        register_driver(name, driver)
