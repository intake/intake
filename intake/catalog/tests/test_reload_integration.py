import os
import os.path
import time

from .util import intake_server
from ..remote import RemoteCatalog

TEST_CATALOG_YAML = os.path.join('/tmp', 'intake_test_catalog.yml')


def setup_module(module):
    with open(TEST_CATALOG_YAML, 'w') as f:
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
    os.remove(TEST_CATALOG_YAML)


def test_reload(intake_server):
    catalog = RemoteCatalog(intake_server)

    entries = catalog.list()
    assert entries == ['use_example1']

    with open(TEST_CATALOG_YAML, 'w') as f:
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

    entries = catalog.list()
    assert entries == ['use_example1', 'use_example1_1']
