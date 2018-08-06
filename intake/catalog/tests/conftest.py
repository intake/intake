import os
import subprocess
import time

import pytest
import requests

from intake.util_tests import ex, PY2
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
        retries = 30
        try:
            while not ping_server(url, swallow_exception=(retries > 1)):
                time.sleep(0.1)
                retries -= 1
        except Exception:
            print(p.communicate())
            raise

        yield 'intake://localhost:%d' % port
    finally:
        if server_conf:
            try:
                env.pop('INTAKE_CONF_FILE', None)
                os.remove(server_conf)
            except:
                pass
        p.terminate()
        p.wait()
        p.kill()
        p.wait()


@pytest.fixture(scope='module')
def http_server():
    if PY2:
        cmd = ['python', '-m', 'SimpleHTTPServer', '8000']
    else:
        cmd = ['python', '-m', 'http.server', '8000']
    p = subprocess.Popen(cmd, cwd=here)
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
