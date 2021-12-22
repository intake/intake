#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import entrypoints
import logging

from ..utils import DriverRegistryView
from .base import DataSource

logger = logging.getLogger('intake')


class DriverRegistry(dict):
    """Dict of driver: DataSource class

    If the value object is a EntryPoint, will load it when accesses, which
    does the import.
    """

    def __getitem__(self, item):
        if isinstance(super().__getitem__(item), entrypoints.EntryPoint):
            self[item] = super().__getitem__(item).load()
        return super().__getitem__(item)


_registry = DriverRegistry()  # internal mutable registry
registry = DriverRegistryView(_registry)  # public, read-ony wrapper


def register_driver(name, driver, overwrite=False):
    """
    Add a driver to intake.registry.

    Parameters
    ----------
    name: string
    driver: DataSource
    overwrite: bool, optional
        False by default.

    Raises
    ------
    ValueError
        If name collides with an existing name in the registry and overwrite is
        False.
    """
    if name in _registry and not overwrite:
        # If we are re-registering the same object, there is no problem.
        original = _registry[name]
        if original is driver:
            return
        raise ValueError(
            f"The driver {driver} could not be registered for the "
            f"name {name} because {_registry[name]} is already "
            f"registered for that name. Use overwrite=True to force it.")
    _registry[name] = driver


def unregister_driver(name):
    """
    Ensure that a given name in the registry is cleared.

    This function is idempotent: if the name does not exist, nothing is done,
    and the function returns None

    Parameters
    ----------
    name: string

    Returns
    -------
    driver: DataSource or None
        Whatever was registered for ``name``, or ``None``
    """
    return _registry.pop(name, None)


# A set of fully-qualified package.module.Class mappings
classes = {}


def import_name(name):
    import importlib
    if ":" in name:
        if name.count(":") > 1:
            raise ValueError("Cannot decipher name to import: %s", name)
        mod, rest = name.split(":")
        bit = importlib.import_module(mod)
        for part in rest.split("."):
            bit = getattr(bit, part)
        return bit
    else:
        mod, cls = name.rsplit('.', 1)
        module = importlib.import_module(mod)
        return getattr(module, cls)


def get_plugin_class(name):
    if name in registry:
        return registry[name]
    if '.' not in name:
        logger.debug('Plugin name "%s" not known' % name)
        return None
    if name not in classes:
        try:
            classes[name] = import_name(name)
        except (KeyError, NameError, ImportError):
            logger.debug('Failed to import "%s"' % name)
    return classes.get(name, None)
