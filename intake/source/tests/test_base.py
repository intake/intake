from collections import defaultdict
import pickle

import pytest
import numpy as np
import pandas as pd
import xarray as xr

from .. import base


def test_plugin_base():
    p = base.Plugin(name='test', version='0.1.0',
                    container='dataframe', partition_access=False)

    assert p.name == 'test'
    assert p.version == '0.1.0'
    assert p.container == 'dataframe'
    assert not p.partition_access
    with pytest.raises(Exception) as except_info:
        p.open()

    assert 'open' in str(except_info.value)


def test_plugin_separate_base_kwargs():
    p = base.Plugin(name='test', version='0.1.0',
                    container='dataframe', partition_access=False)

    base_kwargs, kwargs = p.separate_base_kwargs(dict(a=1, metadata=2))
    assert base_kwargs == dict(metadata=2)
    assert kwargs == dict(a=1)


def test_datasource_base_method_exceptions():
    # Unimplemented methods should raise exceptions
    d = base.DataSource(container='dataframe')

    for (method_name, args) in [('_get_schema', []),
                                ('_get_partition', [1]),
                                ('_close', [])]:
        method = getattr(d, method_name)
        with pytest.raises(Exception) as except_info:
            method(*args)
        assert method_name in str(except_info.value)


def test_datasource_base_context_manager():
    # Base data source should raise a "need to implement" exception when it
    # enters the context manager (which loads the schema)

    with pytest.raises(Exception) as except_info:
        with base.DataSource(container='dataframe'):
            pass
    assert '_get_schema' in str(except_info.value)


class MockDataSourceDataFrame(base.DataSource):
    '''Mock Data Source subclass that returns dataframe containers

    Used to verify that the base DataSource class logic works for dataframes.
    '''
    def __init__(self, a, b):
        self.a = a
        self.b = b

        self.call_count = defaultdict(lambda: 0)

        super(MockDataSourceDataFrame, self).__init__(
            container='dataframe',
            metadata=dict(a=1, b=2)
        )

    def _get_schema(self):
        self.call_count['_get_schema'] += 1

        return base.Schema(datashape='datashape',
                           dtype=np.dtype([('x', np.int64), ('y', np.int64)]),
                           shape=(6,),
                           npartitions=2,
                           extra_metadata=dict(c=3, d=4))

    def _get_partition(self, i):
        self.call_count['_get_partition'] += 1

        if i == 0:
            return pd.DataFrame({'x': [1, 2, 3], 'y': [10, 20, 30]})
        elif i == 1:
            return pd.DataFrame({'x': [4, 5, 6], 'y': [40, 50, 60]})
        else:
            raise Exception('This should never happen')

    def _close(self):
        self.call_count['_close'] += 1


@pytest.fixture
def source_dataframe():
    return MockDataSourceDataFrame(a=1, b=2)


def test_datasource_discover(source_dataframe):
    r = source_dataframe.discover()

    assert source_dataframe.container == 'dataframe'

    row_dtype = np.dtype([('x', np.int64), ('y', np.int64)])
    assert r == {
        'datashape': 'datashape',
        'dtype': row_dtype,
        'shape': (6,),
        'npartitions': 2,
        'metadata': dict(a=1, b=2, c=3, d=4),
    }

    # check attributes have been set
    assert source_dataframe.datashape == 'datashape'
    assert source_dataframe.dtype == row_dtype
    assert source_dataframe.shape == (6,)
    assert source_dataframe.npartitions == 2
    assert source_dataframe.metadata == dict(a=1, b=2, c=3, d=4)

    # check that _get_schema is only called once
    assert source_dataframe.call_count['_get_schema'] == 1
    source_dataframe.discover()
    assert source_dataframe.call_count['_get_schema'] == 1


def check_df(data):
    pd.testing.assert_frame_equal(
        data.reset_index(drop=True),
        pd.DataFrame({
            'x': [1, 2, 3, 4, 5, 6],
            'y': [10, 20, 30, 40, 50, 60]}),
        check_index_type=False,
    )


def test_datasource_read(source_dataframe):
    data = source_dataframe.read()

    check_df(data)


def check_df_parts(parts):
    assert len(parts) == 2
    pd.testing.assert_frame_equal(
        parts[0],
        pd.DataFrame({
            'x': [1, 2, 3],
            'y': [10, 20, 30]})
    )
    pd.testing.assert_frame_equal(
        parts[1],
        pd.DataFrame({
            'x': [4, 5, 6],
            'y': [40, 50, 60]})
    )


def test_datasource_read_chunked(source_dataframe):
    parts = [p for p in source_dataframe.read_chunked()]

    check_df_parts(parts)


def test_datasource_read_partition(source_dataframe):
    source_dataframe.discover()

    parts = [source_dataframe.read_partition(i)
             for i in range(source_dataframe.npartitions)]

    check_df_parts(parts)


def test_datasource_read_partition_out_of_range(source_dataframe):
    with pytest.raises(IndexError):
        source_dataframe.read_partition(-1)

    with pytest.raises(IndexError):
        source_dataframe.read_partition(2)


def test_datasource_to_dask(source_dataframe):
    ddf = source_dataframe.to_dask()

    check_df(ddf.compute())


def test_datasource_close(source_dataframe):
    source_dataframe.close()

    assert source_dataframe.call_count['_close'] == 1


def test_datasource_context_manager(source_dataframe):
    with source_dataframe:
        pass

    assert source_dataframe.call_count['_close'] == 1


def test_datasource_pickle(source_dataframe):
    pkl = pickle.dumps(source_dataframe)
    new_obj = pickle.loads(pkl)

    check_df(new_obj.read())


class MockDataSourcePython(base.DataSource):
    '''Mock Data Source subclass that returns Python list containers

    Used to verify that the base DataSource class logic works for Python lists.
    '''

    def __init__(self, a, b):
        self.a = a
        self.b = b

        self.call_count = defaultdict(lambda: 0)

        super(MockDataSourcePython, self).__init__(
            container='python',
            metadata=dict(a=1, b=2)
        )

    def _get_schema(self):
        self.call_count['_get_schema'] += 1

        return base.Schema(datashape=None,
                           dtype=None,
                           shape=(4,),
                           npartitions=2,
                           extra_metadata=dict(c=3, d=4))

    def _get_partition(self, i):
        self.call_count['_get_partition'] += 1

        if i == 0:
            return [{'x': 'foo', 'y': 'bar'},
                    {'x': 'foo', 'y': 'bar', 'z': 'baz'}]
        elif i == 1:
            return [{'x': 1}, {}]
        else:
            raise Exception('This should never happen')

    def _close(self):
        self.call_count['_close'] += 1


@pytest.fixture
def source_python():
    return MockDataSourcePython(a=1, b=2)


def test_datasource_python_discover(source_python):
    r = source_python.discover()

    assert source_python.container == 'python'

    assert r == {
        'datashape': None,
        'dtype': None,
        'shape': (4,),
        'npartitions': 2,
        'metadata': dict(a=1, b=2, c=3, d=4),
    }

    # check attributes have been set
    assert source_python.datashape is None
    assert source_python.dtype is None
    assert source_python.shape == (4,)
    assert source_python.npartitions == 2
    assert source_python.metadata == dict(a=1, b=2, c=3, d=4)

    # check that _get_schema is only called once
    assert source_python.call_count['_get_schema'] == 1
    source_python.discover()
    assert source_python.call_count['_get_schema'] == 1


def test_datasource_python_read(source_python):
    data = source_python.read()

    assert data == [{'x': 'foo', 'y': 'bar'},
                    {'x': 'foo', 'y': 'bar', 'z': 'baz'},
                    {'x': 1}, {}]


def test_datasource_python_to_dask(source_python):
    db = list(source_python.to_dask())

    assert db == [{'x': 'foo', 'y': 'bar'},
                  {'x': 'foo', 'y': 'bar', 'z': 'baz'},
                  {'x': 1}, {}]

arr = xr.DataArray(np.random.randn(2, 3),
            [('x', ['a', 'b']), ('y', [10, 20, 30])])

class MockDataSourceDataArray(base.DataSource):
    '''Mock Data Source subclass that returns xarray DataArray containers

    Used to verify that the base DataSource class logic works for xarray.
    '''
    def __init__(self, a, b):
        self.a = a
        self.b = b

        self.call_count = defaultdict(lambda: 0)

        super(MockDataSourceDataArray, self).__init__(
            container='dataarray',
            metadata=dict(a=1, b=2)
        )

    def _get_schema(self):
        self.call_count['_get_schema'] += 1

        return base.Schema(datashape='datashape',
                           dtype=np.dtype([('x', np.int64), ('y', np.int64)]),
                           shape=(6,),
                           npartitions=2,
                           extra_metadata=dict(c=3, d=4))

    def _get_partition(self, i):
        self.call_count['_get_partition'] += 1

        if i == 0:
            return arr[:, :1]
        elif i == 1:
            return arr[:, 1:]
        else:
            raise Exception('This should never happen')

    def _close(self):
        self.call_count['_close'] += 1


@pytest.fixture
def source_dataarray():
    return MockDataSourceDataArray(a=1, b=2)

def test_datasource_dataarray_discover(source_dataarray):
    r = source_dataarray.discover()

    assert source_dataarray.container == 'dataarray'

    row_dtype = np.dtype([('x', np.int64), ('y', np.int64)])
    assert r == {
        'datashape': 'datashape',
        'dtype': row_dtype,
        'shape': (6,),
        'npartitions': 2,
        'metadata': dict(a=1, b=2, c=3, d=4),
    }

    # check attributes have been set
    assert source_dataarray.datashape == 'datashape'
    assert source_dataarray.dtype == row_dtype
    assert source_dataarray.shape == (6,)
    assert source_dataarray.npartitions == 2
    assert source_dataarray.metadata == dict(a=1, b=2, c=3, d=4)

    # check that _get_schema is only called once
    assert source_dataarray.call_count['_get_schema'] == 1
    source_dataarray.discover()
    assert source_dataarray.call_count['_get_schema'] == 1

def test_datasource_dataarray_read(source_dataarray):
    data = source_dataarray.read(dim='y')
    assert np.all(arr[0] == data[0])
    assert np.all(arr[1] == data[1])
