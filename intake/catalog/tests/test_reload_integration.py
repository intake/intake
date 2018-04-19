import os
import os.path
import shutil
import tempfile
import time

import pytest

from .util import assert_items_equal
from intake.catalog import Catalog

TMP_DIR = tempfile.mkdtemp()
TEST_CATALOG_PATH = [TMP_DIR]

YAML_FILENAME = 'intake_test_catalog.yml'
MISSING_PATH = os.path.join(TMP_DIR, 'a')


def teardown_module(module):
    shutil.rmtree(TMP_DIR)


@pytest.fixture
def intake_server_with_config(intake_server):
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)

    with open(fullname, 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - dir: '{{ CATALOG_DIR }}/example_plugin_dir'
sources:
  use_example1:
    description: example1 source plugin
    driver: example1
    args: {}
        ''')

    time.sleep(2)

    yield intake_server

    os.remove(fullname)


def test_reload_updated_config(intake_server_with_config):
    catalog = Catalog(intake_server_with_config)

    entries = list(catalog)
    assert entries == ['use_example1']

    with open(os.path.join(TMP_DIR, YAML_FILENAME), 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - dir: '{{ CATALOG_DIR }}/example_plugin_dir'
sources:
  use_example1:
    description: example1 source plugin
    driver: example1
    args: {}
  use_example1_1:
    description: example1 other
    driver: example1
    args: {}
        ''')

    time.sleep(2)

    assert_items_equal(list(catalog), ['use_example1', 'use_example1_1'])


def test_reload_updated_directory(intake_server_with_config):
    catalog = Catalog(intake_server_with_config)

    orig_entries = list(catalog)
    assert 'example2' not in orig_entries

    filename = os.path.join(TMP_DIR, 'intake_test_catalog2.yml')
    with open(filename, 'w') as f:
        f.write('''
sources:
  example2:
    description: source 2
    driver: csv
    args: {}
        ''')

    time.sleep(2)

    assert_items_equal(list(catalog), ['example2'] + orig_entries)


@pytest.fixture
def intake_server_with_missing_dir(intake_server):
    yield MISSING_PATH

    try:
        shutil.rmtree(MISSING_PATH)
    except OSError:
        pass


def test_reload_missing_remote_directory(intake_server_with_missing_dir):
    catalog = Catalog(intake_server_with_missing_dir)
    assert_items_equal(list(catalog), [])

    os.mkdir(MISSING_PATH)
    with open(os.path.join(MISSING_PATH, YAML_FILENAME), 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - dir: '{{ CATALOG_DIR }}/example_plugin_dir'
sources:
  use_example1:
    description: example1 source plugin
    driver: example1
    args: {}
        ''')

    assert_items_equal(list(catalog), ['use_example1'])


@pytest.fixture
def tmpdir():
    path = tempfile.mkdtemp()
    subpath = os.path.join(path, 'a')

    yield subpath

    shutil.rmtree(path)


def test_reload_missing_local_directory(tmpdir):
    catalog = Catalog(tmpdir)
    assert_items_equal(list(catalog), [])

    os.mkdir(tmpdir)
    with open(os.path.join(tmpdir, YAML_FILENAME), 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - dir: '{{ CATALOG_DIR }}/example_plugin_dir'
sources:
  use_example1:
    description: example1 source plugin
    driver: example1
    args: {}
        ''')

    assert_items_equal(list(catalog), ['use_example1'])
