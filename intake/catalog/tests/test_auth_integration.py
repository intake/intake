import os
import os.path
import shutil
import tempfile
import time

import pytest

from .util import assert_items_equal
from intake.catalog import Catalog

from intake.auth.secret import SecretClientAuth

TMP_DIR = tempfile.mkdtemp()
CONF_DIR = os.path.join(TMP_DIR, 'conf')
os.mkdir(CONF_DIR)

TEST_CATALOG_PATH = [TMP_DIR]
YAML_FILENAME = 'intake_test_catalog.yml'


# Create server configuration using shared-secret Auth
TEST_SERVER_CONF = os.path.join(CONF_DIR, 'config.yaml')
conf = '''
auth:
  class: intake.auth.secret.SecretAuth
  kwargs:
    secret: test_secret
'''
with open(TEST_SERVER_CONF, 'w') as f:
    f.write(conf)


def teardown_module(module):
    shutil.rmtree(TMP_DIR)


@pytest.fixture
def intake_server_with_auth(intake_server):
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


def test_secret_auth(intake_server_with_auth):
    auth = SecretClientAuth(secret='test_secret')
    catalog = Catalog(intake_server_with_auth, auth=auth)

    entries = list(catalog)
    assert entries == ['use_example1']


def test_secret_auth_fail(intake_server_with_auth):
    auth = SecretClientAuth(secret='test_wrong_secret')
    with pytest.raises(Exception):
        catalog = Catalog(intake_server_with_auth, auth=auth)
