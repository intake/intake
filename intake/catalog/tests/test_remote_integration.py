import os.path
import pickle

import pytest
import numpy as np
import pandas as pd

from .util import intake_server
from ..remote import RemoteCatalog
from ...source.tests.util import verify_datasource_interface

TEST_CATALOG_YAML = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_info_describe(intake_server):
    catalog = RemoteCatalog(intake_server)

    entries = catalog.list()
    assert entries == ['entry1', 'entry1_part']

    info = catalog.describe('entry1')

    assert info == {
        'container': 'dataframe',
        'description': 'entry1 full',
        'name': 'entry1',
        'user_parameters': []
    }

def test_bad_url(intake_server):
    bad_url = intake_server + '/nonsense_prefix'

    catalog = RemoteCatalog(bad_url)

    with pytest.raises(Exception) as exc_info:
        entries = catalog.list()


def test_unknown_source(intake_server):
    catalog = RemoteCatalog(intake_server)

    with pytest.raises(Exception) as exc_info:
        entries = catalog.describe('does_not_exist')


def test_remote_datasource_interface(intake_server):
    catalog = RemoteCatalog(intake_server)

    d = catalog.get('entry1')

    verify_datasource_interface(d)


def test_read(intake_server):
    catalog = RemoteCatalog(intake_server)

    d = catalog.get('entry1')

    info = d.discover()

    assert info == {
        'datashape': None,
        'dtype': np.dtype([('name', 'O'), ('score', '<f8'), ('rank', '<i8')]),
        'npartitions': 2,
        'shape': (8,)
    }

    df = d.read()
    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))
    
    assert expected_df.equals(df)


def test_read_chunks(intake_server):
    catalog = RemoteCatalog(intake_server)

    d = catalog.get('entry1')

    chunks = list(d.read_chunked())
    assert len(chunks) == 2

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))
    
    assert expected_df.equals(pd.concat(chunks))


def test_read_partition(intake_server):
    catalog = RemoteCatalog(intake_server)

    d = catalog.get('entry1')

    p2 = d.read_partition(1)
    p1 = d.read_partition(0)

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    assert pd.read_csv(file1).equals(p1)
    assert pd.read_csv(file2).equals(p2)
    

def test_close(intake_server):
    catalog = RemoteCatalog(intake_server)

    d = catalog.get('entry1')
    d.close()


def test_pickle(intake_server):
    catalog = RemoteCatalog(intake_server)

    d = catalog.get('entry1')

    new_d = pickle.loads(pickle.dumps(d, pickle.HIGHEST_PROTOCOL))

    df = new_d.read()

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))

    assert expected_df.equals(df)
