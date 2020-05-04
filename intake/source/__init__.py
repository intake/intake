#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import entrypoints
import mimetypes
import logging
logger = logging.getLogger('intake')

from ..utils import DriverRegistryView

# The registry is the mapping of plugin name (aka driver) to DataSource class
_registry = {}  # internal mutable registry
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


# A set of fully-qualifies package.module.Class mappings
classes = {}


def import_name(name):
    import importlib
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


def open_data(url, mime=None, storage_options=None):
    storage_options = storage_options or {}
    if mime is None:
        mime, compression = mimetypes.guess_type(url)
        if compression:
            storage_options['compression'] = compression
    group = entrypoints.get_group_named('intake.mime')
    for name, entrypoint in group.items():
        if name == mime:
            driver = '.'.join([entrypoint.module_name, entrypoint.object_name])
    cls = get_plugin_class(driver)
    return cls(url, storage_options=storage_options)
