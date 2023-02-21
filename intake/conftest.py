# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os
import posixpath
import subprocess
import tempfile
import time

import pytest
import requests

from intake import config, open_catalog, register_driver
from intake.container import persist
from intake.source.base import DataSource, Schema
from intake.tests.test_utils import copy_test_file
from intake.util_tests import PY2, ex
from intake.utils import make_path_posix

here = os.path.dirname(__file__)


MIN_PORT = 7480
MAX_PORT = 7489
PORT = MIN_PORT


class TestSource(DataSource):
    name = "test"
    container = "python"

    def __init__(self, **kwargs):
        self.test_kwargs = kwargs
        super().__init__()

    def _get_schema(self):
        return Schema()


register_driver("test", TestSource)


@pytest.fixture
def tmp_config_path(tmp_path):
    key = "INTAKE_CONF_FILE"
    original = os.getenv(key)
    temp_config_path = os.path.join(tmp_path, "test_config.yml")
    os.environ[key] = temp_config_path
    assert config.cfile() == temp_config_path
    yield temp_config_path
    config.conf.reset()
    if original:
        os.environ[key] = original
    else:
        del os.environ[key]
    assert config.cfile() != temp_config_path


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
    persist.PersistStore().clear()
    os.environ["INTAKE_DEBUG"] = "true"
    # Catalog path comes from the test module
    path = request.module.TEST_CATALOG_PATH
    if isinstance(path, list):
        catalog_path = [p + "/*" for p in path]
    elif isinstance(path, str) and not path.endswith(".yml") and not path.endswith(".yaml"):
        catalog_path = path + "/*"
    else:
        catalog_path = path
    server_conf = getattr(request.module, "TEST_SERVER_CONF", None)

    # Start a catalog server on nonstandard port

    env = dict(os.environ)
    env["INTAKE_TEST"] = "server"
    if server_conf is not None:
        env["INTAKE_CONF_FILE"] = server_conf
    port = pick_port()
    cmd = [ex, "-m", "intake.cli.server", "--sys-exit-on-sigterm", "--port", str(port), "--ttl", "1"]
    if isinstance(catalog_path, list):
        cmd.extend(catalog_path)
    else:
        cmd.append(catalog_path)
    try:
        p = subprocess.Popen(cmd, env=env)
        url = "http://localhost:%d/v1/info" % port

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

        yield "intake://localhost:%d" % port
    finally:
        if server_conf:
            try:
                env.pop("INTAKE_CONF_FILE", None)
                os.remove(server_conf)
            except Exception:
                pass
        p.terminate()
        time.sleep(1)
        p.kill()


@pytest.fixture(scope="module")
def http_server():
    port_as_str = str(pick_port())
    if PY2:
        cmd = ["python", "-m", "SimpleHTTPServer", port_as_str]
    else:
        cmd = ["python", "-m", "http.server", port_as_str]
    p = subprocess.Popen(cmd, cwd=os.path.join(here, "catalog", "tests"))
    url = "http://localhost:{}/".format(port_as_str)
    timeout = 5
    while True:
        try:
            requests.get(url)
            break
        except Exception:
            time.sleep(0.1)
            timeout -= 0.1
            assert timeout > 0, "timeout waiting for http server"
    try:
        yield url
    finally:
        p.terminate()
        p.communicate()


@pytest.fixture(scope="function")
def tempdir():
    import shutil
    import tempfile

    d = make_path_posix(str(tempfile.mkdtemp()))
    try:
        yield d
    finally:
        shutil.rmtree(d)


@pytest.fixture(scope="function")
def temp_cache(tempdir):
    import intake
    from intake.container.persist import store

    old = intake.config.conf.copy()
    olddir = intake.config.confdir
    intake.config.confdir = tempdir
    intake.config.conf.update({"cache_dir": make_path_posix(str(tempdir)), "cache_download_progress": False, "cache_disabled": False})
    intake.config.conf.save()
    store.__init__(os.path.join(tempdir, "persist"))
    try:
        yield
    finally:
        intake.config.confdir = olddir
        intake.config.conf.update(old)
        intake.config.save_conf()


@pytest.fixture(scope="function")
def env(temp_cache, tempdir):
    import intake

    env = os.environ.copy()
    env["INTAKE_CONF_DIR"] = intake.config.confdir
    env["INTAKE_CACHE_DIR"] = intake.config.conf["cache_dir"]
    return env


@pytest.fixture
def inherit_params_cat():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = posixpath.join(tmp_dir, "intake")
        target_catalog = copy_test_file("catalog_inherit_params.yml", tmp_path)
        return open_catalog(target_catalog)


@pytest.fixture
def inherit_params_multiple_cats():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = posixpath.join(tmp_dir, "intake")
        copy_test_file("catalog_inherit_params.yml", tmp_path)
        copy_test_file("catalog_nested_sub.yml", tmp_path)
        return open_catalog(tmp_path + "/*.yml")


@pytest.fixture
def inherit_params_subcat():
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = posixpath.join(tmp_dir, "intake")
        target_catalog = copy_test_file("catalog_inherit_params.yml", tmp_path)
        copy_test_file("catalog_nested_sub.yml", tmp_path)
        return open_catalog(target_catalog)
