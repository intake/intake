# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from importlib.metadata import entry_points
import logging
import re
import warnings

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
            eps = entry_points()
            if hasattr(eps, "select"):  # Python 3.10+ / importlib_metadata >= 3.9.0
                specs = eps.select(group="intake.drivers")
            else:
                specs = eps.get("intake.drivers", [])
            # must cache this, since lookup if fairly slow and we do this a lot
            self._entrypoints = list(specs)
        return self._entrypoints

    def from_conf(self):
        return []

    def __setitem__(self, key, value):
        super(DriverSouces, self).__setitem__(key, value)
        self.save()

    def __delitem__(self, key):
        super(DriverSouces, self).__delitem__(key)
        self.save()

    @property
    def scanned(self):
        return {}

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
        """Add runtime driver definition to list of registered drivers

        (drivers in global scope with corresponding ``intake.open_*`` function)

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
        raise ConfigurationError(
            f"Failed to load {entrypoint.name} driver because module "
            f"{entrypoint.module_name} could not be imported."
        ) from err
    except AttributeError as err:
        raise ConfigurationError(
            f"Failed to load {entrypoint.name} driver because no object "
            f"named {entrypoint.object_name} could be found in the module "
            f"{entrypoint.module_name}."
        ) from err


def _normalize(name):
    if not name.isidentifier():
        # primitive name normalization
        name = re.sub("[-=~^&|@+]", "_", name)
    if not name.isidentifier():
        warnings.warn('Invalid Intake plugin name "%s" found.', name, stacklevel=2)

    return name


class ConfigurationError(Exception):
    pass
