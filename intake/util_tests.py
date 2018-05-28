from contextlib import contextmanager
import os
import requests
import shutil
import subprocess
import sys
import tempfile
import time
import yaml

ex = sys.executable
here = os.path.dirname(__file__)
defcat = os.path.join(here, 'cli', 'server', 'tests', 'catalog1.yml')
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
        fn = os.path.join(d, 'conf.yaml')
        with open(fn, 'w') as f:
            yaml.dump(conf, f)
        yield fn


@contextmanager
def server(args=None, cat=None, env=None, wait=None, timeout=25):
    cat = cat if cat is not None else defcat
    args = list(args if args is not None else []) + []
    env = env if env is not None else {}
    cmd = [ex, '-m', 'intake.cli.server'] + list(args) + [cat]
    p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    if wait is not None:
        while True:
            try:
                requests.get('http://localhost:%i/v1/info' % wait)
                break
            except:
                time.sleep(0.1)
                timeout -= 0.1
                assert timeout > 0
    try:
        yield p
    finally:
        p.terminate()
