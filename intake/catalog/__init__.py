#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from .base import Catalog
from .local import MergedCatalog, EntrypointsCatalog
from .default import load_combo_catalog

builtin = None

def _make_builtin():
    return MergedCatalog(
        [EntrypointsCatalog(), load_combo_catalog()],
        name='builtin',
        description='Generated from data packages found on your intake search path')
