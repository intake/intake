#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from collections import defaultdict
import pickle

import pytest
import numpy as np
import pandas as pd

import intake.source.base as base
import intake.source.derived as der
import intake.source


def test_datasource_base_method_exceptions():
    # Unimplemented methods should raise exceptions
    d = base.DataSource()

    for (method_name, args) in [('_get_schema', []),
                                ('_get_partition', [1]),
                                ('_close', [])]:
        method = getattr(d, method_name)
        with pytest.raises(NotImplementedError):
            method(*args)


def test_name():
    d = base.DataSource()
    assert d.classname == 'intake.source.base.DataSource'
    assert isinstance(hash(d), int)


def test_datasource_base_context_manager():
    # Base data source should raise a "need to implement" exception when it
    # enters the context manager (which loads the schema)

    with pytest.raises(NotImplementedError):
        with base.DataSource():
            pass


class MockDataSourceDataFrame(base.DataSource):
    """Mock Data Source subclass that returns dataframe containers

    Used to verify that the base DataSource class logic works for dataframes.
    """

    container = 'dataframe'
    name = 'mock'

    def __init__(self, a, b):
        self.a = a
        self.b = b

        self.call_count = defaultdict(lambda: 0)

        super(MockDataSourceDataFrame, self).__init__(
            metadata=dict(a=1, b=2)
        )
        self.npartitions = 2

    def _get_schema(self):
        self.call_count['_get_schema'] += 1

        return base.Schema(dtype=np.dtype([('x', np.int64), ('y', np.int64)]),
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

    def read(self):
        return pd.concat([self._get_partition(i)
                          for i in range(self.npartitions)])

    def to_dask(self):
        import dask.dataframe as dd
        import dask
        return dd.from_delayed([dask.delayed(self._get_partition)(i)
                                for i in range(self.npartitions)])

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
        'dtype': row_dtype,
        'shape': (6,),
        'npartitions': 2,
        'metadata': dict(a=1, b=2, c=3, d=4),
    }

    # check attributes have been set
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
    """Mock Data Source subclass that returns Python list containers

    Used to verify that the base DataSource class logic works for Python lists.
    """
    container = 'python'
    name = 'mock'

    def __init__(self, a, b):
        self.a = a
        self.b = b

        self.call_count = defaultdict(lambda: 0)

        super(MockDataSourcePython, self).__init__(
            metadata=dict(a=1, b=2)
        )
        self.npartitions = 2

    def _get_schema(self):
        self.call_count['_get_schema'] += 1

        return base.Schema(dtype=None,
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

    def read(self):
        return sum([self._get_partition(i) for i in range(self.npartitions)],
               [])

    def to_dask(self):
        import dask.bag as db
        import dask
        return db.from_delayed([dask.delayed(self._get_partition)(i)
                                for i in range(self.npartitions)])

    def _close(self):
        self.call_count['_close'] += 1


@pytest.fixture
def source_python():
    return MockDataSourcePython(a=1, b=2)


def test_datasource_python_discover(source_python):
    r = source_python.discover()

    assert source_python.container == 'python'

    assert r == {
        'dtype': None,
        'shape': (4,),
        'npartitions': 2,
        'metadata': dict(a=1, b=2, c=3, d=4),
    }

    # check attributes have been set
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


def test_yaml_method(source_python):
    out = source_python.yaml()
    assert 'mock' in out  # the "driver"
    assert 'metadata' in out
    assert 'a: 1' in out


def test_alias_fail():
    s = der.AliasSource('atarget')
    s.container == 'other'
    with pytest.raises(ValueError):
        s.read()


def test_reconfigure():
    s = MockDataSourcePython(a=1, b=2)
    assert s.a == 1
    s2 = s(a=3, b=4)
    assert s2.a == 3
    s3 = s2(a=4)
    assert s3.a == 4
    assert s3.b == 4


@pytest.mark.parametrize("data", [
    ("intake.source.import_name", intake.source.import_name),
    ("intake.source:import_name", intake.source.import_name),
    ("intake.source.DataSource", intake.source.DataSource),
    ("intake.source:DataSource", intake.source.DataSource),
    ("intake.source:base.DataSource", intake.source.DataSource),
])
def test_import_name(data):
    text, object = data
    assert intake.source.import_name(text) == object
