import os.path
import pickle

import pytest
import numpy as np
import pandas as pd

from ...source.tests.util import verify_datasource_interface
from .util import assert_items_equal
from intake.catalog import Catalog

TEST_CATALOG_YAML = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_info_describe(intake_server):
    catalog = Catalog(intake_server)

    assert_items_equal(catalog.get_entries(), ['use_example1', 'entry1', 'entry1_part'])

    info = catalog['entry1'].describe()

    assert info == {
        'container': 'dataframe',
        'description': 'entry1 full',
        'name': 'entry1',
        'direct_access': 'forbid',
        'user_parameters': []
    }

    info = catalog['entry1_part'].describe()

    assert info['direct_access'] == 'allow'


def test_bad_url(intake_server):
    bad_url = intake_server + '/nonsense_prefix'

    with pytest.raises(Exception):
        Catalog(bad_url)


def test_unknown_source(intake_server):
    catalog = Catalog(intake_server)

    with pytest.raises(Exception):
        catalog['does_not_exist'].describe()


def test_remote_datasource_interface(intake_server):
    catalog = Catalog(intake_server)

    d = catalog['entry1'].get()

    verify_datasource_interface(d)


def test_read(intake_server):
    catalog = Catalog(intake_server)

    d = catalog['entry1'].get()

    info = d.discover()

    assert info == {
        'datashape': None,
        'dtype': np.dtype([('name', 'O'), ('score', '<f8'), ('rank', '<i8')]),
        'npartitions': 2,
        'shape': (None,)  # Do not know CSV size ahead of time
    }

    assert d.metadata == dict(foo='bar', bar=[1, 2, 3])

    df = d.read()
    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))

    assert not d.direct  # this should be proxied

    assert expected_df.equals(df)


def test_read_direct(intake_server):
    catalog = Catalog(intake_server)

    d = catalog['entry1_part'].get(part='2')

    info = d.discover()

    assert info == {
        'datashape': None,
        'dtype': np.dtype([('name', 'O'), ('score', '<f8'), ('rank', '<i8')]),
        'npartitions': 1,
        'shape': (None,), # do not know size of CSV ahead of time
        'metadata': {'bar': [2, 4, 6], 'foo': 'baz'}
    }

    assert d.metadata == dict(foo='baz', bar=[2, 4, 6])
    assert d.description == 'entry1 part'
    df = d.read()
    test_dir = os.path.dirname(__file__)
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.read_csv(file2)

    assert d.direct  # this should be direct

    assert expected_df.equals(df)


def test_read_chunks(intake_server):
    catalog = Catalog(intake_server)

    d = catalog.entry1.get()

    chunks = list(d.read_chunked())
    assert len(chunks) == 2

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))

    assert expected_df.equals(pd.concat(chunks))


def test_read_partition(intake_server):
    catalog = Catalog(intake_server)

    d = catalog.entry1.get()

    p2 = d.read_partition(1)
    p1 = d.read_partition(0)

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    assert pd.read_csv(file1).equals(p1)
    assert pd.read_csv(file2).equals(p2)


def test_close(intake_server):
    catalog = Catalog(intake_server)

    d = catalog.entry1.get()
    d.close()


def test_pickle(intake_server):
    catalog = Catalog(intake_server)

    d = catalog.entry1.get()

    new_d = pickle.loads(pickle.dumps(d, pickle.HIGHEST_PROTOCOL))

    df = new_d.read()

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))

    assert expected_df.equals(df)


def test_to_dask(intake_server):
    catalog = Catalog(intake_server)
    d = catalog.entry1.get()
    df = d.to_dask()

    assert df.npartitions == 2
