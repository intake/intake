#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os.path
import pickle

import pytest
import pandas as pd

from intake.source.tests.util import verify_datasource_interface
from .util import assert_items_equal
from intake import open_catalog
from intake.catalog.remote import RemoteCatalog

TEST_CATALOG_PATH = os.path.join(os.path.dirname(__file__), 'catalog1.yml')


def test_info_describe(intake_server):
    catalog = open_catalog(intake_server)

    assert_items_equal(list(catalog), ['use_example1', 'nested', 'entry1',
                                       'entry1_part', 'remote_env',
                                       'local_env', 'text', 'arr', 'datetime'])

    info = catalog['entry1'].describe()

    expected = {
        'container': 'dataframe',
        'description': 'entry1 full',
        'name': 'entry1',
        'direct_access': 'forbid',
        'user_parameters': []
    }
    for k in expected:
        assert info[k] == expected[k]

    info = catalog['entry1_part'].describe()

    assert info['direct_access'] == 'allow'


def test_bad_url(intake_server):
    bad_url = intake_server + '/nonsense_prefix'

    with pytest.raises(Exception):
        open_catalog(bad_url)


def test_metadata(intake_server):
    catalog = open_catalog(intake_server)
    assert hasattr(catalog, 'metadata')
    assert catalog.metadata['test'] is True
    assert catalog.version == 1


def test_nested_remote(intake_server):
    from intake.catalog.local import LocalCatalogEntry
    catalog = open_catalog()
    catalog._entries = {
        'server': LocalCatalogEntry('server', 'remote test', 'intake_remote',
                                    True, {'url': intake_server}, [],
                                    [], {}, None)
    }
    assert 'entry1' in catalog.server()


def test_remote_direct(intake_server):
    from intake.container.dataframe import RemoteDataFrame
    catalog = open_catalog(intake_server)
    s0 = catalog.entry1()
    s0.discover()
    s = RemoteDataFrame(intake_server.replace('intake', 'http'), {},
                        name='entry1', parameters={},
                        npartitions=s0.npartitions,
                        shape=s0.shape,
                        metadata=s0.metadata,
                        dtype=s0.dtype)
    assert s0.read().equals(s.read())


def test_entry_metadata(intake_server):
    catalog = open_catalog(intake_server)
    entry = catalog['arr']
    assert entry.metadata == entry().metadata


def test_unknown_source(intake_server):
    catalog = open_catalog(intake_server)

    with pytest.raises(Exception):
        catalog['does_not_exist'].describe()


def test_remote_datasource_interface(intake_server):
    catalog = open_catalog(intake_server)

    d = catalog['entry1']

    verify_datasource_interface(d)


def test_environment_evaluation(intake_server):
    catalog = open_catalog(intake_server)
    import os
    os.environ['INTAKE_TEST'] = 'client'
    catalog['remote_env']


def test_read(intake_server):
    catalog = open_catalog(intake_server)

    d = catalog['entry1']

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))
    meta = expected_df[:0]

    info = d.discover()
    assert info['dtype'] == {k: str(v) for k, v
                             in meta.dtypes.to_dict().items()}
    assert info['npartitions'] == 2
    assert info['shape'] == (None, 3)  # Do not know CSV size ahead of time

    md = d.metadata.copy()
    assert md['foo'] == 'bar'
    assert md['bar'] == [1, 2, 3]

    df = d.read()

    assert expected_df.equals(df)


def test_read_direct(intake_server):
    catalog = open_catalog(intake_server)

    d = catalog['entry1_part'].configure_new(part='2')
    test_dir = os.path.dirname(__file__)
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.read_csv(file2)
    meta = expected_df[:0]

    info = d.discover()

    assert info['dtype'] == {k: str(v) for k, v
                             in meta.dtypes.to_dict().items()}
    assert info['npartitions'] == 1
    assert info['shape'] == (None, 3)  # Do not know CSV size ahead of time
    md = info['metadata'].copy()
    md.pop('catalog_dir', None)
    assert md == {'bar': [2, 4, 6], 'foo': 'baz'}

    md = d.metadata.copy()
    md.pop('catalog_dir', None)
    assert md == dict(foo='baz', bar=[2, 4, 6])
    assert d.description == 'entry1 part'
    df = d.read()

    assert expected_df.equals(df)


def test_read_chunks(intake_server):
    catalog = open_catalog(intake_server)

    d = catalog.entry1

    chunks = list(d.read_chunked())
    assert len(chunks) == 2

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))

    assert expected_df.equals(pd.concat(chunks))


def test_read_partition(intake_server):
    catalog = open_catalog(intake_server)

    d = catalog.entry1

    p2 = d.read_partition(1)
    p1 = d.read_partition(0)

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    assert pd.read_csv(file1).equals(p1)
    assert pd.read_csv(file2).equals(p2)


def test_close(intake_server):
    catalog = open_catalog(intake_server)

    d = catalog.entry1
    d.close()


def test_with(intake_server):
    catalog = open_catalog(intake_server)

    with catalog.entry1 as f:
        assert f.discover()


def test_pickle(intake_server):
    catalog = open_catalog(intake_server)

    d = catalog.entry1

    new_d = pickle.loads(pickle.dumps(d, pickle.HIGHEST_PROTOCOL))

    df = new_d.read()

    test_dir = os.path.dirname(__file__)
    file1 = os.path.join(test_dir, 'entry1_1.csv')
    file2 = os.path.join(test_dir, 'entry1_2.csv')
    expected_df = pd.concat((pd.read_csv(file1), pd.read_csv(file2)))

    assert expected_df.equals(df)


def test_to_dask(intake_server):
    catalog = open_catalog(intake_server)
    d = catalog.entry1
    df = d.to_dask()

    assert df.npartitions == 2


def test_remote_env(intake_server):
    import os
    os.environ['INTAKE_TEST'] = 'client'
    catalog = open_catalog(intake_server)
    catalog.remote_env
    with pytest.raises(Exception) as e:
        catalog.remote_env.read()

    with pytest.raises(Exception) as e:
        catalog.local_env
    assert 'path-client' in str(e.value)

    # prevents *client* from getting env
    catalog = open_catalog(intake_server, getenv=False)
    with pytest.raises(Exception) as e:
        catalog.local_env
    assert 'INTAKE_TEST' in str(e.value)


def test_remote_sequence(intake_server):
    import glob
    d = os.path.dirname(TEST_CATALOG_PATH)
    catalog = open_catalog(intake_server)
    assert 'text' in catalog
    s = catalog.text()
    s.discover()
    assert s.npartitions == len(glob.glob(os.path.join(d, '*.yml')))
    assert s.read_partition(0)
    assert s.read()


def test_remote_arr(intake_server):
    catalog = open_catalog(intake_server)
    assert 'arr' in catalog
    s = catalog.arr()
    s.discover()
    assert 'remote-array' in s.to_dask().name
    assert s.npartitions == 2
    assert s.read_partition(0).ravel().tolist() == list(range(50))
    assert s.read().ravel().tolist() == list(range(100))


def test_pagination(intake_server):
    PAGE_SIZE = 2
    catalog = open_catalog(intake_server, page_size=PAGE_SIZE)
    assert len(catalog._entries._page_cache) == 0
    assert len(catalog._entries._direct_lookup_cache) == 0
    # Trigger fetching one specific name.
    catalog['arr']
    assert len(catalog._entries._page_cache) == 0
    assert len(catalog._entries._direct_lookup_cache) == 1
    # Using `in` on a Catalog should not iterate.
    'arr' in catalog
    assert len(catalog._entries._page_cache) == 0
    assert len(catalog._entries._direct_lookup_cache) == 1
    # Trigger fetching just one full page.
    list(zip(range(PAGE_SIZE), catalog))
    assert len(catalog._entries._page_cache) == PAGE_SIZE
    assert len(catalog._entries._direct_lookup_cache) == 1
    # Trigger fetching all pages by list-ifying.
    list(catalog)
    assert len(catalog._entries._page_cache) > PAGE_SIZE
    assert len(catalog._entries._direct_lookup_cache) == 1
    # Now direct lookup by name should be free because everything is cached.
    catalog['text']
    assert len(catalog._entries._direct_lookup_cache) == 1


def test_dir(intake_server):
    PAGE_SIZE = 2
    catalog = open_catalog(intake_server, page_size=PAGE_SIZE)
    assert len(catalog._entries._page_cache) == 0
    assert len(catalog._entries._direct_lookup_cache) == 0
    assert not catalog._entries.complete

    with pytest.warns(UserWarning, match="Tab-complete"):
        key_completions = catalog._ipython_key_completions_()
    with pytest.warns(UserWarning, match="Tab-complete"):
        dir_ = dir(catalog)
    # __dir__ triggers loading the first page.
    assert len(catalog._entries._page_cache) == 2
    assert len(catalog._entries._direct_lookup_cache) == 0
    assert not catalog._entries.complete
    assert set(key_completions) == set(['use_example1', 'nested'])
    assert 'metadata' in dir_  # a normal attribute
    assert 'use_example1' in dir_  # an entry from the first page
    assert 'arr' not in dir_  # an entry we haven't cached yet

    # Trigger fetching one specific name.
    catalog['arr']
    with pytest.warns(UserWarning, match="Tab-complete"):
        dir_ = dir(catalog)
    with pytest.warns(UserWarning, match="Tab-complete"):
        key_completions = catalog._ipython_key_completions_()
    assert 'metadata' in dir_
    assert 'arr' in dir_  # an entry cached via direct access
    assert 'arr' in key_completions

    # Load everything.
    list(catalog)
    assert catalog._entries.complete
    with pytest.warns(None) as record:
        assert set(catalog) == set(catalog._ipython_key_completions_())
        assert set(catalog).issubset(set(dir(catalog)))
    assert len(record) == 0

    # Load without pagination (with also loads everything).
    catalog = open_catalog(intake_server, page_size=None)
    assert catalog._entries.complete
    with pytest.warns(None) as record:
        assert set(catalog) == set(catalog._ipython_key_completions_())
        assert set(catalog).issubset(set(dir(catalog)))
    assert len(record) == 0


def test_getitem_and_getattr(intake_server):
    catalog = open_catalog(intake_server)
    catalog['arr']
    with pytest.raises(KeyError):
        catalog['doesnotexist']
    with pytest.raises(KeyError):
        catalog['_doesnotexist']
    with pytest.raises(KeyError):
        # This exists as an *attribute* but not as an item.
        catalog['metadata']
    catalog.arr  # alias to catalog['arr']
    catalog.metadata  # a normal attribute
    with pytest.raises(AttributeError):
        catalog.doesnotexit
    with pytest.raises(AttributeError):
        catalog._doesnotexit
    assert catalog.arr.describe() == catalog['arr'].describe()
    assert "RemoteArray" in catalog.arr.classname
    assert isinstance(catalog.metadata, (dict, type(None)))


def test_search(intake_server):
    remote_catalog = open_catalog(intake_server)
    local_catalog = open_catalog(TEST_CATALOG_PATH)

    # Basic example
    remote_results = remote_catalog.search('entry1')
    local_results = local_catalog.search('entry1')
    expected = ['nested.entry1', 'nested.entry1_part', 'entry1', 'entry1_part']
    assert isinstance(remote_results, RemoteCatalog)
    assert list(local_results) == list(remote_results) == expected

    # Progressive search
    remote_results = remote_catalog.search('entry1').search('part')
    local_results = local_catalog.search('entry1').search('part')
    expected = ['nested.entry1_part', 'entry1_part']
    assert isinstance(remote_results, RemoteCatalog)
    assert list(local_results) == list(remote_results) == expected

    # Double progressive search
    remote_results = (remote_catalog
        .search('entry1')
        .search('part')
        .search('part'))
    local_results = (local_catalog
        .search('entry1')
        .search('part')
        .search('part'))
    expected = ['nested.entry1_part', 'entry1_part']
    assert isinstance(remote_results, RemoteCatalog)
    assert list(local_results) == list(remote_results) == expected

    # Search on a nested Catalog.
    remote_results = remote_catalog['nested'].search('entry1')
    local_results = local_catalog['nested'].search('entry1')
    expected = ['nested.entry1', 'nested.entry1_part', 'entry1', 'entry1_part']
    assert isinstance(remote_results, RemoteCatalog)
    assert list(local_results) == list(remote_results) == expected

    # Search with empty results set
    remote_results = remote_catalog.search('DOES NOT EXIST')
    local_results = local_catalog.search('DOES NOT EXIST')
    expected = []
    assert isinstance(remote_results, RemoteCatalog)
    assert list(local_results) == list(remote_results) == expected


def test_access_subcatalog(intake_server):
    catalog = open_catalog(intake_server)
    catalog['nested']


def test_len(intake_server):
    remote_catalog = open_catalog(intake_server)
    local_catalog = open_catalog(TEST_CATALOG_PATH)
    assert sum(1 for entry in local_catalog) == len(remote_catalog)


def test_datetime(intake_server):
    catalog = open_catalog(intake_server)
    info = catalog["datetime"].describe()
    catalog['datetime'].parameters['time'] == pd.Timestamp("1970")


