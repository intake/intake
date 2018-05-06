import os.path
import pickle

import pytest
import numpy as np
import pandas as pd

from ...source.tests.util import verify_datasource_interface
from .util import assert_items_equal
from intake.catalog import Catalog

TEST_CATALOG_PATH = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_info_describe(intake_server):
    catalog = Catalog(intake_server)

    assert_items_equal(list(catalog), ['use_example1', 'entry1', 'entry1_part',
                                       'remote_env', 'local_env'])

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


def test_metadata(intake_server):
    catalog = Catalog(intake_server)
    assert hasattr(catalog, 'metadata')
    assert catalog['metadata']['test'] is True
    assert catalog.version == 1


def test_unknown_source(intake_server):
    catalog = Catalog(intake_server)

    with pytest.raises(Exception):
        catalog['does_not_exist'].describe()


def test_remote_datasource_interface(intake_server):
    catalog = Catalog(intake_server)

    d = catalog['entry1'].get()

    verify_datasource_interface(d)


def test_environment_evaluation(intake_server):
    catalog = Catalog(intake_server)
    import os
    os.environ['INTAKE_TEST'] = 'client'
    d = catalog['remote_env']


def test_read(intake_server):
    catalog = Catalog(intake_server)

    d = catalog['entry1'].get()

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))
    meta = expected_df[:0]

    info = d.discover()
    assert info['datashape'] is None
    assert info['dtype'].equals(meta)
    assert info['npartitions'] == 2
    assert info['shape'] == (None, 3)  # Do not know CSV size ahead of time

    assert d.metadata == dict(foo='bar', bar=[1, 2, 3])

    df = d.read()

    assert not d.direct  # this should be proxied

    assert expected_df.equals(df)


def test_read_direct(intake_server):
    catalog = Catalog(intake_server)

    d = catalog['entry1_part'].get(part='2')
    test_dir = os.path.dirname(__file__)
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.read_csv(file2)
    meta = expected_df[:0]

    info = d.discover()

    assert info['datashape'] is None
    assert info['dtype'].equals(meta)
    assert info['npartitions'] == 1
    assert info['shape'] == (None, 3)  # Do not know CSV size ahead of time
    assert info['metadata'] == {'bar': [2, 4, 6], 'foo': 'baz'}

    assert d.metadata == dict(foo='baz', bar=[2, 4, 6])
    assert d.description == 'entry1 part'
    df = d.read()

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


def test_with(intake_server):
    catalog = Catalog(intake_server)

    with catalog.entry1.get() as f:
        assert f.discover()


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


def test_remote_env(intake_server):
    import os
    os.environ['INTAKE_TEST'] = 'client'
    catalog = Catalog(intake_server)
    s = catalog.remote_env.get()
    assert 'INTAKE_TEST' in s._user_parameters['intake_test']

    s = catalog.local_env.get()
    assert 'client' == s._user_parameters['intake_test']

    # prevents *client* from getting env
    catalog = Catalog(intake_server, getenv=False)
    s = catalog.local_env.get()
    assert 'INTAKE_TEST' in s._user_parameters['intake_test']
