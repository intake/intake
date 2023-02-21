# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager

import requests
import yaml

from .utils import make_path_posix

ex = sys.executable
here = os.path.dirname(__file__)
defcat = make_path_posix(os.path.join(here, "cli", "server", "tests", "catalog1.yml"))
PY2 = sys.version_info[0] == 2


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


@contextmanager
def server(args=None, cat=None, env=None, wait=None, timeout=25):
    cat = cat if cat is not None else defcat
    args = list(args if args is not None else []) + []
    env = env if env is not None else {}
    cmd = [ex, "-m", "intake.cli.server"] + list(args) + [cat]
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if wait is not None:
        while True:
            try:
                requests.get("http://localhost:%i/v1/info" % wait)
                break
            except:
                time.sleep(0.1)
                timeout -= 0.1
                assert timeout > 0
    try:
        yield p
    finally:
        p.terminate()
