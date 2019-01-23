#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import subprocess
import time

import pytest
import requests

from intake.util_tests import ex, PY2
from intake.utils import make_path_posix

here = os.path.dirname(__file__)


MIN_PORT = 7480
MAX_PORT = 7489
PORT = MIN_PORT


def ping_server(url, swallow_exception, head=None):
    try:
        r = requests.get(url)
    except Exception as e:
        if swallow_exception:
            return False
        else:
            raise e

    return r.status_code in (200, 403)  # allow forbidden as well


def pick_port():
    global PORT
    port = PORT
    if port == MAX_PORT:
        PORT = MIN_PORT
    else:
        PORT += 1

    return port


@pytest.fixture(scope="module")
def intake_server(request):
    os.environ['INTAKE_DEBUG'] = 'true'
    # Catalog path comes from the test module
    path = request.module.TEST_CATALOG_PATH
    if isinstance(path, list):
        catalog_path = [p + '/*' for p in path]
    elif isinstance(path, str) and not path.endswith(
            '.yml') and not path.endswith('.yaml'):
        catalog_path = path + '/*'
    else:
        catalog_path = path
    server_conf = getattr(request.module, 'TEST_SERVER_CONF', None)

    # Start a catalog server on nonstandard port

    env = dict(os.environ)
    env['INTAKE_TEST'] = 'server'
    if server_conf is not None:
        env['INTAKE_CONF_FILE'] = server_conf
    port = pick_port()
    cmd = [ex, '-m', 'intake.cli.server', '--sys-exit-on-sigterm',
           '--port', str(port)]
    if isinstance(catalog_path, list):
        cmd.extend(catalog_path)
    else:
        cmd.append(catalog_path)
    try:
        p = subprocess.Popen(cmd, env=env)
        url = 'http://localhost:%d/v1/info' % port

        # wait for server to finish initalizing, but let the exception through
        # on last retry
        retries = 300
        try:
            while not ping_server(url, swallow_exception=(retries > 1)):
                time.sleep(0.1)
                retries -= 1
        except Exception:
            print(p.communicate())
            raise
        assert retries > 0, "Server never appeared"

        yield 'intake://localhost:%d' % port
    finally:
        if server_conf:
            try:
                env.pop('INTAKE_CONF_FILE', None)
                os.remove(server_conf)
            except:
                pass
        p.terminate()
        time.sleep(1)
        p.kill()


@pytest.fixture(scope='module')
def http_server():
    if PY2:
        cmd = ['python', '-m', 'SimpleHTTPServer', '8000']
    else:
        cmd = ['python', '-m', 'http.server', '8000']
    p = subprocess.Popen(cmd, cwd=os.path.join(here, 'catalog', 'tests'))
    timeout = 5
    while True:
        try:
            requests.get('http://localhost:8000/')
            break
        except:
            time.sleep(0.1)
            timeout -= 0.1
            assert timeout > 0, "timeout waiting for http server"
    try:
        yield 'http://localhost:8000/'
    finally:
        p.terminate()
        p.communicate()


@pytest.fixture(scope='function')
def tempdir():
    import tempfile
    import shutil
    d = make_path_posix(str(tempfile.mkdtemp()))
    try:
        yield d
    finally:
        shutil.rmtree(d)


@pytest.fixture(scope='function')
def temp_cache(tempdir):
    import intake
    old = intake.config.conf.copy()
    olddir = intake.config.confdir
    intake.config.confdir = tempdir
    intake.config.conf.update({'cache_dir': make_path_posix(str(tempdir)),
                               'cache_download_progress': False,
                               'cache_disabled': False})
    intake.config.save_conf()
    try:
        yield
    finally:
        intake.config.confdir = olddir
        intake.config.conf.update(old)


@pytest.fixture(scope='function')
def env(temp_cache, tempdir):
    import intake
    env = os.environ.copy()
    env["INTAKE_CONF_DIR"] = intake.config.confdir
    env['INTAKE_CACHE_DIR'] = intake.config.conf['cache_dir']
    return env
