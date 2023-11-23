# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import os
import posixpath
import tempfile

import pytest

from intake import config, open_catalog, register_driver
from intake.source.base import DataSource, Schema
from intake.tests.test_utils import copy_test_file

here = os.path.dirname(__file__)


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
    from fsspec.implementations.local import make_path_posix

    key = "INTAKE_CONF_FILE"
    original = os.getenv(key)
    temp_config_path = make_path_posix(os.path.join(tmp_path, "test_config.yml"))
    os.environ[key] = temp_config_path
    assert config.cfile() == temp_config_path
    yield temp_config_path
    config.conf.reset()
    if original:
        os.environ[key] = original
    else:
        del os.environ[key]
    assert config.cfile() != temp_config_path


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
