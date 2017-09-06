import os
import subprocess
import time

import pytest

@pytest.fixture(scope="module")
def intake_server(request):
    # Catalog URL comes from the test module
    catalog_yaml = request.module.TEST_CATALOG_YAML

    # Start a catalog server on nonstandard port
    port = 7483
    cmd = 'python -m intake.catalog --port %d %s' % (port, catalog_yaml)
    env = dict(os.environ)

    p = subprocess.Popen(cmd, shell=True, env=env)
    time.sleep(2)  # wait for server to finish initializing

    yield 'http://localhost:%d' % (port,)
    p.terminate()
