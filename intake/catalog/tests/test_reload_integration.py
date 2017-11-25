import os
import os.path
import shutil
import tempfile
import time

from .util import assert_items_equal
from intake.catalog import Catalog

TMP_DIR = tempfile.mkdtemp()
TEST_CATALOG_YAML = [TMP_DIR]

YAML_FILENAME = 'intake_test_catalog.yml'


def setup_module(module):
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)
    with open(fullname, 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - dir: !template '{{ CATALOG_DIR }}/example_plugin_dir'
sources:
  use_example1:
    description: example1 source plugin
    driver: example1
    args: {}
        ''')


def teardown_module(module):
    shutil.rmtree(TMP_DIR)


def test_reload(intake_server):
    catalog = Catalog(intake_server)

    entries = catalog.get_entries()
    assert entries == ['use_example1']

    with open(os.path.join(TMP_DIR, YAML_FILENAME), 'w') as f:
        f.write('''
plugins:
  source:
    - module: intake.catalog.tests.example1_source
    - dir: !template '{{ CATALOG_DIR }}/example_plugin_dir'
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

    assert_items_equal(catalog.get_entries(), ['use_example1', 'use_example1_1'])


def test_reload_newfile(intake_server):
    catalog = Catalog(intake_server)

    orig_entries = catalog.get_entries()
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

    assert_items_equal(catalog.get_entries(), ['example2'] + orig_entries)
