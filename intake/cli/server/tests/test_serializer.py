import os.path

import pytest
import numpy as np
import pandas as pd

from intake.container import serializer


all_serializers = pytest.mark.parametrize(
    'ser', serializer.format_registry.values())
all_compressors = pytest.mark.parametrize(
    'comp', serializer.compression_registry.values())


@all_serializers
def test_dataframe(ser):
    csv_filename = os.path.join(os.path.dirname(__file__), 'entry1_1.csv')
    expected_df = pd.read_csv(csv_filename)

    # Check roundtrip
    df = ser.decode(ser.encode(expected_df, 'dataframe'), 'dataframe')

    assert expected_df.equals(df)


@all_serializers
def test_ndarray(ser):
    expected_array = np.arange(35).reshape((5, 7))

    # Check roundtrip
    array = ser.decode(ser.encode(expected_array, 'ndarray'), 'ndarray')
    np.testing.assert_array_equal(expected_array, array)


@all_serializers
def test_python(ser):
    expected = [dict(a=1, b=[1, 2], c='str'), dict(a=[1, 2], b='str', d=None)]
    actual = ser.decode(ser.encode(expected, 'python'), 'python')

    assert expected == actual


@all_compressors
def test_compression_roundtrip(comp):
    data = b'1234\x01\x02'

    assert data == comp.decompress(comp.compress(data))


def test_none_compress():
    data = b'1234\x01\x02'
    comp = serializer.NoneCompressor()

    # None should be no-op
    assert data == comp.decompress(data)
    assert data == comp.compress(data)
