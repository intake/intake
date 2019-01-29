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
