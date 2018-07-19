import os
import os.path
import shutil
import tempfile
import time

import pytest

from .util import assert_items_equal
from intake import Catalog

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


@pytest.fixture
def intake_server_with_auth(intake_server):
    fullname = os.path.join(TMP_DIR, YAML_FILENAME)

    try:
        os.makedirs(os.path.join(TMP_DIR, 'data'))
    except:
        pass
    with open(fullname, 'w') as f:
        f.write('''
sources:
  example:
    description: example1 source plugin
    driver: csv
    args:
      urlpath: "{{ CATALOG_DIR }}/data/example.csv"
        ''')

    csv_name = os.path.join(TMP_DIR, 'data', 'example.csv')
    with open(csv_name, 'w') as f:
        f.write('a,b,c\n1,2,3\n4,5,6')
    time.sleep(2)

    yield intake_server

    try:
        shutil.rmtree(TMP_DIR)
    except:
        pass


def test_secret_auth(intake_server_with_auth):
    auth = SecretClientAuth(secret='test_secret')
    catalog = Catalog(intake_server_with_auth, auth=auth)

    entries = list(catalog)
    assert entries == ['example']

    catalog.example.read()


def test_secret_auth_fail(intake_server_with_auth):
    auth = SecretClientAuth(secret='test_wrong_secret')
    with pytest.raises(Exception):
        Catalog(intake_server_with_auth, auth=auth)
