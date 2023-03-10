# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import importlib
import inspect
import logging
import pkgutil
import re
import time
import warnings

import entrypoints

from ..config import conf

logger = logging.getLogger("intake")


class DriverSouces:
    """
    Handles the various ways in which drivers can be known to Intake
    """

    def __init__(self, config=None, do_scan=None):
        """

        Parameters
        ----------
        config: intake.config.Config oinstance, optional
            If not given, will use ``intake.config.conf`` singleton
        do_scan: bool or None
            Whether to scan packages with names ``intake_*`` for valid drivers.
            This is for backward compatibility only. If not given, value comes
            from the config key "package_scan", default False.
        """
        self.conf = config or conf
        self.do_scan = do_scan
        self._scanned = None
        self._entrypoints = None
        self._registered = None
        self._disabled = None

    @property
    def package_scan(self):
        return self.conf.get("package_scan", False) if self.do_scan is None else self.do_scan

    @package_scan.setter
    def package_scan(self, val):
        self.conf["package_scan"] = val

    def from_entrypoints(self):
        if self._entrypoints is None:
            # must cache this, since lookup if fairly slow and we do this a lot
            self._entrypoints = list(entrypoints.get_group_all("intake.drivers"))
        return self._entrypoints

    def from_conf(self):
        return [
            entrypoints.EntryPoint(
                name=k, module_name=v.rsplit(":", 1)[0] if ":" in v else v.rsplit(".", 1)[0], object_name=v.rsplit(":", 1)[1] if ":" in v else v.rsplit(".", 1)[1]
            )
            for k, v in self.conf.get("drivers", {}).items()
            if v
        ]

    def __setitem__(self, key, value):
        super(DriverSouces, self).__setitem__(key, value)
        self.save()

    def __delitem__(self, key):
        super(DriverSouces, self).__delitem__(key)
        self.save()

    @property
    def scanned(self):
        # cache since imports should not change during session
        if self._scanned is None:
            # these are already classes
            self._scanned = _package_scan()
        return self._scanned

    def disabled(self):
        if self._disabled is None:
            self._disabled = {k for k, v in self.conf.get("drivers", {}).items() if v is False}

        return self._disabled

    def registered(self):
        # priority order (decreasing): runtime, config, entrypoints, package scan
        if self._registered is None:
            out = {}
            if self.package_scan:
                out.update(self.scanned)

            for ep in self.from_entrypoints() + self.from_conf():
                out[ep.name] = ep

            self._registered = out

        return self._registered

    def enabled_plugins(self):
        return {k: v for k, v in self.registered().items() if k not in self.disabled()}

    def register_driver(self, name, value, clobber=False, do_enable=False):
        """Add runtime driver definition to list of registered drivers (drivers in global scope with corresponding ``intake.open_*`` function)

        Parameters
        ----------
        name: str
            Name of the driver
        value: str, entrypoint or class
            Pointer to the implementation
        clobber: bool
            If True, perform the operation even if the driver exists
        do_enable: bool
            If True, unset the disabled flag for this driver
        """
        name = _normalize(name)
        if name in self.registered() and not clobber:
            raise ValueError(f"Driver {name} already enabled")
        if name in self.disabled():
            if do_enable:
                self.enable(name, value)
            else:
                logger.warning(f"Adding driver {name}, but it is disabled")

        self.registered()[name] = value

    def unregister_driver(self, name):
        """Remove runtime registered driver"""
        name = _normalize(name)
        self.registered().pop(name)

    def enable(self, name, driver=None):
        """
        Explicitly assign a driver to a name, or remove ban

        Updates the associated config, which will be persisted

        Parameters
        ----------
        name : string
            As in ``'zarr'``
        driver : string
            Dotted object name, as in ``'intake_xarray.xzarr.ZarrSource'``.
            If None, simply remove driver disable flag, if it is found
        """
        config = self.conf
        if "drivers" not in config:
            config["drivers"] = {}
        if driver:
            config["drivers"][name] = driver
        elif config["drivers"].get(name) is False:
            del config["drivers"][name]
        if name in self.disabled():
            self.disabled().remove(name)
        config.save()

    def disable(self, name):
        """Disable a driver by name.

        Updates the associated config, which will be persisted

        Parameters
        ----------
        name : string
            As in ``'zarr'``
        """
        name = _normalize(name)
        config = self.conf
        if "drivers" not in config:
            config["drivers"] = {}
        config["drivers"][name] = False
        config.save()
        self.disabled().add(name)


drivers = DriverSouces()


def _load_entrypoint(entrypoint):
    """
    Call entrypoint.load() and, if it fails, raise context-specific errors.
    """
    try:
        return entrypoint.load()
    except ImportError as err:
        raise ConfigurationError(f"Failed to load {entrypoint.name} driver because module " f"{entrypoint.module_name} could not be imported.") from err
    except AttributeError as err:
        raise ConfigurationError(
            f"Failed to load {entrypoint.name} driver because no object "
            f"named {entrypoint.object_name} could be found in the module "
            f"{entrypoint.module_name}."
        ) from err


def _package_scan(path=None, plugin_prefix="intake_"):
    """Scan for intake drivers and return a dict of plugins.

    This searches path (or sys.path) for packages with names that start with
    plugin_prefix.  Those modules will be imported and scanned for subclasses
    of intake.source.base.Plugin.  Any subclasses found will be instantiated
    and returned in a dictionary, with the plugin's name attribute as the key.
    """
    warnings.warn("Package scanning may be removed", category=PendingDeprecationWarning)

    plugins = {}

    for importer, name, ispkg in pkgutil.iter_modules(path=path):
        if name.startswith(plugin_prefix):
            t = time.time()
            new_plugins = load_plugins_from_module(name)

            for plugin_name, plugin in new_plugins.items():
                if plugin_name in plugins:
                    orig_path = inspect.getfile(plugins[plugin_name])
                    new_path = inspect.getfile(plugin)
                    warnings.warn(
                        'Plugin name collision for "%s" from'
                        "\n    %s"
                        "\nand"
                        "\n    %s"
                        "\nKeeping plugin from first location." % (plugin_name, orig_path, new_path)
                    )
                else:
                    plugins[plugin_name] = plugin
            logger.debug("Import %s took: %7.2f s" % (name, time.time() - t))
    return plugins


def _normalize(name):
    if not name.isidentifier():
        # primitive name normalization
        name = re.sub("[-=~^&|@+]", "_", name)
    if not name.isidentifier():
        warnings.warn('Invalid Intake plugin name "%s" found.', name, stacklevel=2)

    return name


def load_plugins_from_module(module_name):
    """Imports a module and returns dictionary of discovered Intake plugins.

    Plugin classes are instantiated and added to the dictionary, keyed by the
    name attribute of the plugin object.
    """
    from intake.catalog import Catalog
    from intake.source import DataSource

    plugins = {}

    try:
        try:
            mod = importlib.import_module(module_name)
        except ImportError as error:
            if module_name.endswith(".py"):
                # Provide a specific error regarding the removal of behavior
                # that intake formerly supported.
                raise ImportError(
                    "Intake formerly supported executing arbitrary Python "
                    "files not on the sys.path. This is no longer supported. "
                    "Drivers must be specific with a module path like "
                    "'package_name.module_name, not a Python filename like "
                    "'module.py'."
                ) from error
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
