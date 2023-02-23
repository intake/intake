# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import logging
from collections.abc import MappingView

import entrypoints

from .base import DataSource
from .discovery import drivers

logger = logging.getLogger("intake")


class DriverRegistry(MappingView):
    """Dict of driver: DataSource class

    If the value object is a EntryPoint or str, will load it when accesses, which
    does the import.
    """

    def __init__(self, drivers_source=drivers):
        self.drivers = drivers_source

    def __getitem__(self, item):
        it = self.drivers.enabled_plugins()[item]
        if isinstance(it, entrypoints.EntryPoint):
            return it.load()
        elif isinstance(it, str):
            return import_name(it)
        elif issubclass(it, DataSource):
            return it
        raise ValueError

    def __iter__(self):
        return iter(self.drivers.enabled_plugins())

    def keys(self):
        return list(self)

    def __len__(self):
        return len(self.drivers.enabled_plugins())

    def __repr__(self):
        return """<Intake driver registry>"""

    def __contains__(self, item):
        return item in self.keys()


registry = DriverRegistry()
register_driver = drivers.register_driver
unregister_driver = drivers.unregister_driver

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
        mod, cls = name.rsplit(".", 1)
        module = importlib.import_module(mod)
        return getattr(module, cls)


def get_plugin_class(name):
    if name in registry:
        return registry[name]
    if "." not in name:
        logger.debug('Plugin name "%s" not known' % name)
        return None
    if name not in classes:
        try:
            classes[name] = import_name(name)
        except (KeyError, NameError, ImportError):
            logger.debug('Failed to import "%s"' % name)
    return classes.get(name, None)
