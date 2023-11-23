# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os
import shutil
import tempfile
from contextlib import contextmanager
import yaml


@contextmanager
def tempdir():
    d = tempfile.mkdtemp()
    try:
        yield d
    finally:
        if os.path.exists(d):
            shutil.rmtree(d)


@contextmanager
def temp_conf(conf):
    with tempdir() as d:
        fn = os.path.join(d, "conf.yaml")
        with open(fn, "w") as f:
            yaml.dump(conf, f)
        yield fn
