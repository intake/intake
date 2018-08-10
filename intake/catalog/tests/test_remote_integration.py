import os.path
import pickle

import pytest
import pandas as pd

from ...source.tests.util import verify_datasource_interface
from .util import assert_items_equal
from intake import Catalog

TEST_CATALOG_PATH = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_info_describe(intake_server):
    catalog = Catalog(intake_server)

    assert_items_equal(list(catalog), ['use_example1', 'nested', 'entry1',
                                       'entry1_part', 'remote_env',
                                       'local_env', 'text', 'arr'])

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
    assert info['dtype'] == {k: str(v) for k, v
                             in meta.dtypes.to_dict().items()}
    assert info['npartitions'] == 2
    assert info['shape'] == (None, 3)  # Do not know CSV size ahead of time

    assert d.metadata == dict(foo='bar', bar=[1, 2, 3], cache=[])

    df = d.read()

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
    assert info['dtype'] == {k: str(v) for k, v
                             in meta.dtypes.to_dict().items()}
    assert info['npartitions'] == 1
    assert info['shape'] == (None, 3)  # Do not know CSV size ahead of time
    assert info['metadata'] == {'bar': [2, 4, 6], 'foo': 'baz', 'cache': []}

    assert d.metadata == dict(foo='baz', bar=[2, 4, 6], cache=[])
    assert d.description == 'entry1 part'
    df = d.read()

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
    with pytest.raises(Exception) as e:
        catalog.remote_env.get()
    assert 'path-server' in str(e.value)

    with pytest.raises(Exception) as e:
        catalog.local_env.get()
    assert 'path-client' in str(e.value)

    # prevents *client* from getting env
    catalog = Catalog(intake_server, getenv=False)
    with pytest.raises(Exception) as e:
        catalog.local_env.get()
    assert 'INTAKE_TEST' in str(e.value)


def test_remote_sequence(intake_server):
    import glob
    d = os.path.dirname(TEST_CATALOG_PATH)
    catalog = Catalog(intake_server)
    assert 'text' in catalog
    s = catalog.text()
    s.discover()
    assert s.npartitions == len(glob.glob(os.path.join(d, '*.yml')))
    assert s.read_partition(0)
    assert s.read()


def test_remote_arr(intake_server):
    catalog = Catalog(intake_server)
    assert 'arr' in catalog
    s = catalog.arr()
    s.discover()
    assert 'remote-array' in s.to_dask().name
    assert s.npartitions == 2
    assert s.read_partition(0).ravel().tolist() == list(range(50))
    assert s.read().ravel().tolist() == list(range(100))
