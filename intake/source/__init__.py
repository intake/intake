#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import logging
logger = logging.getLogger('intake')

# The registry is the mapping of plugin name (aka driver) to DataSource class
registry = {}

# A set of fully-qualifies package.module.Class mappings
classes = {}


def get_plugin_class(name):
    if name in registry:
        return registry[name]
    if '.' not in name:
        logger.debug('Plugin name "%s" not known' % name)
        return None
    if name not in classes:
        import importlib
        mod, cls = name.rsplit('.', 1)
        try:
            module = importlib.import_module(mod)
        except ImportError:
            logger.debug('Import of plugin module "%s" failed' % mod)
            return None
        try:
            classes[name] = getattr(module, cls)
        except (KeyError, NameError):
            logger.debug('Class "%s" not found in module "%s"' % (cls, mod))
    return classes.get(name, None)
