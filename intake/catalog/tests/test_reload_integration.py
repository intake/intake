#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os
import os.path
import shutil
import tempfile
import time

import pytest

from .util import assert_items_equal
from intake import open_catalog

TMP_DIR = tempfile.mkdtemp()
TEST_CATALOG_PATH = [TMP_DIR]

YAML_FILENAME = 'intake_test_catalog.yml'
MISSING_PATH = os.path.join(TMP_DIR, 'a')


def teardown_module(module):
    try:
        shutil.rmtree(TMP_DIR)
    except:
        pass


@pytest.fixture
def intake_server_with_config(intake_server):
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)

    with open(fullname, 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - module: intake.catalog.tests.example_plugin_dir.example2_source
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
    catalog = open_catalog(intake_server_with_config, ttl=0.1)

    entries = list(catalog)
    assert entries == ['use_example1']

    with open(os.path.join(TMP_DIR, YAML_FILENAME), 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - module: intake.catalog.tests.example_plugin_dir.example2_source
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

    time.sleep(1.2)

    assert_items_equal(list(catalog), ['use_example1', 'use_example1_1'])


def test_reload_updated_directory(intake_server_with_config):
    catalog = open_catalog(intake_server_with_config, ttl=0.1)

    orig_entries = list(catalog)
    assert 'example2' not in orig_entries

    filename = os.path.join(TMP_DIR, 'intake_test_catalog2.yml')
    with open(filename, 'w') as f:
        f.write('''
sources:
  example2:
    description: source 2
    driver: csv
    args:
        urlpath: none
        ''')

    time.sleep(1.2)

    assert_items_equal(list(catalog), ['example2'] + orig_entries)


def test_reload_missing_remote_directory(intake_server):
    try:
        shutil.rmtree(TMP_DIR)
    except:
        pass

    time.sleep(1)
    catalog = open_catalog(intake_server, ttl=0.1)
    assert_items_equal(list(catalog), [])

    os.mkdir(TMP_DIR)
    with open(os.path.join(TMP_DIR, YAML_FILENAME), 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - module: intake.catalog.tests.example_plugin_dir.example2_source
sources:
  use_example1:
    description: example1 source plugin
    driver: example1
    args: {}
        ''')
    time.sleep(1.2)

    assert_items_equal(list(catalog), ['use_example1'])
    try:
        shutil.rmtree(TMP_DIR)
    except:
        pass


def test_reload_missing_local_directory(tempdir):
    catalog = open_catalog(tempdir + '/*', ttl=0.1)
    assert_items_equal(list(catalog), [])

    with open(os.path.join(tempdir, YAML_FILENAME), 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - module: intake.catalog.tests.example_plugin_dir.example2_source
sources:
  use_example1:
    description: example1 source plugin
    driver: example1
    args: {}
        ''')

    time.sleep(1.2)
    assert 'use_example1' in catalog
