import os.path
import pickle

import pytest
import pandas as pd

from .. import csv
from .util import verify_plugin_interface, verify_datasource_interface


@pytest.fixture
def data_filenames():
    basedir = os.path.dirname(__file__)
    return dict(sample1=os.path.join(basedir, 'sample1.csv'),
                sample2_1=os.path.join(basedir, 'sample2_1.csv'),
                sample2_2=os.path.join(basedir, 'sample2_2.csv'),
                sample2_all=os.path.join(basedir, 'sample2_*.csv'))


@pytest.fixture
def sample1_datasource(data_filenames):
    return csv.CSVSource(data_filenames['sample1'])


@pytest.fixture
def sample2_datasource(data_filenames):
    return csv.CSVSource(data_filenames['sample2_all'])


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


def test_read_chunked(sample1_datasource, data_filenames):
    expected_df = pd.read_csv(data_filenames['sample1'])

    parts = list(sample1_datasource.read_chunked())
    df = pd.concat(parts)

    assert expected_df.equals(df)


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
    assert isinstance(sample1_datasource.plot(), holoviews.NdOverlay)


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
