#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import pytest
import datetime
from intake.source.utils import (
    path_to_glob, path_to_pattern, reverse_format, reverse_formats)


@pytest.mark.parametrize('pattern,expected', [
    ('data/LT05_L1TP_042033_{start_date:%Y%m%d}_{end_date:%Y%m%d}_01_T1_sr_band{band:1d}.tif',
     'data/LT05_L1TP_042033_*_*_01_T1_sr_band*.tif'),
    ('data/LT05_L1TP_042033_*_*_01_T1_sr_band*.tif',
     'data/LT05_L1TP_042033_*_*_01_T1_sr_band*.tif'),
    ('{year}/{month}/{day}.csv', '*/*/*.csv'),
    ('data/**/*.csv', 'data/**/*.csv'),
    ('data/{year:4}{month:02}{day:02}.csv', 'data/*.csv'),
    ('{lone_param}','*')
])
def test_path_to_glob(pattern, expected):
    assert path_to_glob(pattern) == expected


@pytest.mark.parametrize('pattern,resolved,expected', [
    ('*.csv', 'apple.csv', {}),
    ('{}.csv', 'apple.csv', {}),
    ('{fruit}.{}', 'apple.csv', {'fruit': 'apple'}),
    ('data//{fruit}.csv', 'data/apple.csv', {'fruit': 'apple'}),
    ('data\\{fruit}.csv', 'C:\\data\\apple.csv', {'fruit': 'apple'}),
    ('data/{fruit}.csv', 'C:\\data\\apple.csv', {'fruit': 'apple'}),
    ('data/{fruit}.csv', 'data//apple.csv', {'fruit': 'apple'}),
    ('{num:d}.csv', 'k.csv', {'num': 'k'}),
    ('{year:d}/{month:d}/{day:d}.csv', '2016/2/01.csv',
    {'year': 2016, 'month': 2, 'day': 1}),
    ('{year:.4}/{month:.2}/{day:.2}.csv', '2016/2/01.csv',
    {'year': '2016', 'month': '2', 'day': '01'}),
    ('SRLCCTabularDat/Ecoregions_{emissions}_Precip_{model}.csv',
     '/user/examples/SRLCCTabularDat/Ecoregions_a1b_Precip_ECHAM5-MPI.csv',
     {'emissions':'a1b', 'model': 'ECHAM5-MPI'}),
    ('data_{date:%Y_%m_%d}.csv', 'data_2016_10_01.csv',
     {'date': datetime.datetime(2016, 10, 1, 0, 0)}),
    ('{state}{zip:5}', 'PA19104',
    {'state': 'PA', 'zip': '19104'}),
    ('{state}{zip:5d}.csv', 'PA19104.csv',
    {'state': 'PA', 'zip': 19104}),
    ('{state:2}{zip:d}.csv', 'PA19104.csv',
    {'state': 'PA', 'zip': 19104}),
])
def test_reverse_format(pattern, resolved, expected):
    assert reverse_format(pattern, resolved) == expected


@pytest.mark.parametrize('pattern,expected', [
    ('{date:%Y%m%d}', {'date': datetime.datetime(2016, 10, 1)}),
    ('{num: .2f}', {'num': 0.23}),
    ('{percentage:.2%}', {'percentage': 0.23}),
    ('data/{year:4d}{month:02d}{day:02d}.csv',
     {'year': 2016, 'month': 10, 'day': 1})
])
def test_roundtripping_reverse_format(pattern, expected):
    resolved = pattern.format(**expected)
    actual = reverse_format(pattern, resolved)
    assert actual == expected


def test_reverse_format_errors():
    pattern = '{month}{day}{year}'
    resolved = '20012001'
    with pytest.raises(ValueError, match=("Format specifier must be set "
                                          "if no separator between fields.")):
        reverse_format(pattern, resolved)

    pattern = '{month:.2}{day:2}{year:.4}'
    resolved = '20012001'
    with pytest.raises(ValueError, match="Format specifier must have a set width"):
        reverse_format(pattern, resolved)

    pattern = '{band!s}'
    resolved = '1'
    with pytest.raises(ValueError, match="Conversion not allowed. Found on band"):
        reverse_format(pattern, resolved)

    pattern = 'data_{band}'
    resolved = '1'
    with pytest.raises(ValueError, match=("Resolved string must match "
                                            "pattern. 'data_' not found.")):

        reverse_format(pattern, resolved)


paths = ['data_2014_01_03.csv', 'data_2014_02_03.csv', 'data_2015_12_03.csv']
@pytest.mark.parametrize('pattern', [
    'data_{year}_{month}_{day}.csv',
    'data_{year:d}_{month:02d}_{day:02d}.csv',
    'data_{date:%Y_%m_%d}.csv'
])
def test_roundtrip_reverse_formats(pattern):
    args = reverse_formats(pattern, paths)
    for i, path  in enumerate(paths):
        assert pattern.format(
            **{field: values[i] for field, values in  args.items()}) == path


@pytest.mark.parametrize('path,metadata,expected', [
    ('http://data/band{band:1d}.tif',
     {'cache': [{'argkey': 'urlpath', 'regex': 'data'}]},
     '/band{band:1d}.tif'),
    ('/data/band{band:1d}.tif', {},
     '/data/band{band:1d}.tif'),
    ('/data/band{band:1d}.tif', None,
     '/data/band{band:1d}.tif')
])
def test_path_to_pattern(path, metadata, expected):
    assert path_to_pattern(path, metadata) == expected
