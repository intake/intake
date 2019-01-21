#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import os.path
import posixpath
import pickle

import pytest
import pandas as pd

import intake.source.csv as csv
from intake.utils import make_path_posix
from .util import verify_plugin_interface, verify_datasource_interface


@pytest.fixture
def data_filenames():
    basedir = make_path_posix(os.path.dirname(__file__))
    return dict(sample1=posixpath.join(basedir, 'sample1.csv'),
                sample2_1=posixpath.join(basedir, 'sample2_1.csv'),
                sample2_2=posixpath.join(basedir, 'sample2_2.csv'),
                sample2_all=posixpath.join(basedir, 'sample2_*.csv'),
                sample_pattern=posixpath.join(basedir, 'sample{num:d}_{dup:d}.csv'))


@pytest.fixture
def sample1_datasource(data_filenames):
    return csv.CSVSource(data_filenames['sample1'])


@pytest.fixture
def sample2_datasource(data_filenames):
    return csv.CSVSource(data_filenames['sample2_all'])


@pytest.fixture
def sample_pattern_datasource(data_filenames):
    return csv.CSVSource(data_filenames['sample_pattern'])

@pytest.fixture
def sample_list_datasource(data_filenames):
    return csv.CSVSource([data_filenames['sample2_1'], data_filenames['sample2_2']])

@pytest.fixture
def sample_list_datasource_with_path_as_pattern_str(data_filenames):
    return csv.CSVSource(
        [data_filenames['sample2_1'], data_filenames['sample2_2']],
        path_as_pattern='sample{num:d}_{dup:d}.csv')

@pytest.fixture
def sample_pattern_datasource_with_cache(data_filenames):
    metadata = {'cache': [{'argkey': 'urlpath',
                           'regex': make_path_posix(
                                os.path.dirname(__file__)),
                           'type': 'file'}]}
    return csv.CSVSource(data_filenames['sample_pattern'], metadata=metadata)


def test_csv_plugin():
    p = csv.CSVSource
    assert isinstance(p.version, str)
    assert p.container == 'dataframe'
    verify_plugin_interface(p)


def test_open(data_filenames):
    d = csv.CSVSource(data_filenames['sample1'])
    assert d.container == 'dataframe'
    assert d.description is None
    verify_datasource_interface(d)


def test_discover(sample1_datasource):
    info = sample1_datasource.discover()

    assert info['dtype'] == {'name': 'object', 'score': 'float64',
                             'rank': 'int64'}
    # Do not know length without parsing CSV
    assert info['shape'] == (None, 3)
    assert info['npartitions'] == 1


def test_read(sample1_datasource, data_filenames):
    expected_df = pd.read_csv(data_filenames['sample1'])
    df = sample1_datasource.read()

    assert expected_df.equals(df)

def test_read_list(sample_list_datasource,  data_filenames):
    df_1 = pd.read_csv(data_filenames['sample2_1'])
    df_2 = pd.read_csv(data_filenames['sample2_2'])
    expected_df = pd.concat([df_1, df_2])
    df = sample_list_datasource.read()

    assert expected_df.equals(df)

def test_read_chunked(sample1_datasource, data_filenames):
    expected_df = pd.read_csv(data_filenames['sample1'])

    parts = list(sample1_datasource.read_chunked())
    df = pd.concat(parts)

    assert expected_df.equals(df)


def check_read_pattern_output(source):
    da = source.to_dask()
    assert da.num.cat.known is True
    assert da.dup.cat.known is True

    # check that first partition has correct num and dup; which file
    # it represents is not guaranteed
    df0 = da.get_partition(0).compute()
    if df0['name'][0].endswith('1'):
        assert all(df0.num == 2)
        assert all(df0.dup == 1)
    elif df0['name'][0].endswith('2'):
        assert all(df0.num == 2)
        assert all(df0.dup == 2)
    elif df0['name'][0].endswith('3'):
        assert all(df0.num == 3)
        assert all(df0.dup == 2)

    df = source.read()
    assert len(df.columns) == 5
    assert 'num' in df
    assert 'dup' in df
    assert df.num.dtype == 'category'
    assert df.dup.dtype == 'category'

    names = ['Alice', 'Bob', 'Charlie', 'Eve']

    file_1 = df[df['name'].isin(['{}1'.format(name) for name in names])]
    assert all(file_1.num == 2)
    assert all(file_1.dup == 1)

    file_2 = df[df['name'].isin(['{}2'.format(name) for name in names])]
    assert all(file_2.num == 2)
    assert all(file_2.dup == 2)

    file_3 = df[df['name'].isin(['{}3'.format(name) for name in names])]
    assert all(file_3.num == 3)
    assert all(file_3.dup == 2)

    return da


def test_read_pattern(sample_pattern_datasource):
    da = sample_pattern_datasource.to_dask()
    assert set(da.num.cat.categories) == {2, 3}
    assert set(da.dup.cat.categories) == {1, 2}
    check_read_pattern_output(sample_pattern_datasource)


def test_read_pattern_with_cache(sample_pattern_datasource_with_cache):
    da = sample_pattern_datasource_with_cache.to_dask()
    assert set(da.num.cat.categories) == {2, 3}
    assert set(da.dup.cat.categories) == {1, 2}
    check_read_pattern_output(sample_pattern_datasource_with_cache)


def test_read_pattern_with_path_as_pattern_str(sample_list_datasource_with_path_as_pattern_str):
    da = sample_list_datasource_with_path_as_pattern_str.to_dask()
    assert set(da.num.cat.categories) == {2}
    assert set(da.dup.cat.categories) == {1, 2}
    check_read_pattern_output(sample_list_datasource_with_path_as_pattern_str)


def test_read_partition(sample2_datasource, data_filenames):
    expected_df1 = pd.read_csv(data_filenames['sample2_1'])
    expected_df2 = pd.read_csv(data_filenames['sample2_2'])

    sample2_datasource.discover()
    assert sample2_datasource.npartitions == 2

    # Read partitions is opposite order
    df2 = sample2_datasource.read_partition(1)
    df1 = sample2_datasource.read_partition(0)

    assert expected_df1.equals(df1)
    assert expected_df2.equals(df2)


def test_to_dask(sample1_datasource, data_filenames):
    dd = sample1_datasource.to_dask()
    df = dd.compute()

    expected_df = pd.read_csv(data_filenames['sample1'])

    assert expected_df.equals(df)


def test_plot(sample1_datasource):
    pytest.importorskip('hvplot')
    import holoviews

    p = sample1_datasource.plot()
    assert isinstance(p, holoviews.NdOverlay)


def test_close(sample1_datasource, data_filenames):
    sample1_datasource.close()
    # Can reopen after close
    df = sample1_datasource.read()
    expected_df = pd.read_csv(data_filenames['sample1'])

    assert expected_df.equals(df)


def test_pickle(sample1_datasource):
    pickled_source = pickle.dumps(sample1_datasource)
    sample1_clone = pickle.loads(pickled_source)

    expected_df = sample1_datasource.read()
    df = sample1_clone.read()

    assert expected_df.equals(df)
