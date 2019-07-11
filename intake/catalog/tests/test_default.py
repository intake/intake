#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from pathlib import Path
import sys
from intake.catalog import default
from intake.catalog.base import Catalog


def test_which():
    p = default.which('python')
    assert Path(p).resolve() == Path(sys.executable).resolve()


def test_load():
    cat = default.load_user_catalog()
    assert isinstance(cat, Catalog)
    cat = default.load_global_catalog()
    assert isinstance(cat, Catalog)
