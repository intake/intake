import os
import subprocess
import time

import pytest
import requests


MIN_PORT = 7480
MAX_PORT = 7489
PORT = MIN_PORT

def ping_server(url, swallow_exception):
    try:
        r = requests.get(url)
    except Exception as e:
        if swallow_exception:
            return False
        else:
            raise e

    return r.status_code == 200


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
    # Catalog URL comes from the test module
    catalog_yaml = request.module.TEST_CATALOG_YAML

    # Start a catalog server on nonstandard port
    port = pick_port()
    cmd = ['python', '-m', 'intake.catalog', '--sys-exit-on-sigterm', '--port', str(port), catalog_yaml]
    env = dict(os.environ)

    p = subprocess.Popen(cmd, env=env)
    url = 'http://localhost:%d' % (port,)

    # wait for server to finish initalizing, but let the exception through on last retry
    retries = 100
    while not ping_server(url, swallow_exception=(retries > 1)):
        time.sleep(0.1) 
        retries -= 1    

    yield url
    p.terminate()
    p.wait()
