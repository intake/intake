#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from .base import Catalog
from .local import MergedCatalog, EntrypointsCatalog
from .default import load_combo_catalog


def _make_builtin():
    return MergedCatalog(
        [EntrypointsCatalog(), load_combo_catalog()],
        name='builtin',
        description='Generated from data packages found on your intake search path')


def __getattr__(name):
    """Only make the builtin catalog on request"""
    global builtin
    if name == "builtin":
        builtin = _make_builtin()
        return builtin
    raise AttributeError(name)
