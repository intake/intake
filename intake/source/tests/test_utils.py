import pytest
from datetime import datetime
from intake.source.utils import pattern_to_glob, reverse_format


@pytest.mark.parametrize('pattern,expected', [
    ('data/LT05_L1TP_042033_{start_date:%Y%m%d}_{end_date:%Y%m%d}_01_T1_sr_band{band:1d}.tif',
     'data/LT05_L1TP_042033_*_*_01_T1_sr_band*.tif'),
    ('data/LT05_L1TP_042033_*_*_01_T1_sr_band*.tif',
     'data/LT05_L1TP_042033_*_*_01_T1_sr_band*.tif'),
    ('{year}/{month}/{day}.csv', '*/*/*.csv'),
    ('data/**/*.csv', 'data/**/*.csv'),
    ('data/{year:4}{month:02}{day:02}.csv', 'data/*.csv'),
])
def test_pattern_to_glob(pattern, expected):
    assert pattern_to_glob(pattern) == expected


@pytest.mark.parametrize('pattern,resolved,expected', [
    ('*.csv', 'apple.csv', {}),
    ('{num:d}.csv', 'k.csv', {'num': 'k'}),
    ('{year:d}/{month:d}/{day:d}.csv', '2016/2/01.csv',
    {'year': 2016, 'month': 2, 'day': 1}),
    ('{year:.4}/{month:.2}/{day:.2}.csv', '2016/2/01.csv',
    {'year': '2016', 'month': '2', 'day': '01'}),
    ('SRLCCTabularDat/Ecoregions_{emissions}_Precip_{model}.csv',
     '/user/examples/SRLCCTabularDat/Ecoregions_a1b_Precip_ECHAM5-MPI.csv',
     {'emissions':'a1b', 'model': 'ECHAM5-MPI'}),
])
def test_reverse_format(pattern, resolved, expected):
    assert reverse_format(pattern, resolved) == expected


@pytest.mark.parametrize('pattern,expected', [
    ('{date:%Y%m%d}', {'date': datetime(2016, 10, 1)}),
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
    with pytest.raises(ValueError, message=("Format specifier must be set "
                                            "if no separator between fields.")):
        reverse_format(pattern, resolved)

    pattern = '{month:.2}{day:2}{year:.4}'
    resolved = '20012001'
    with pytest.raises(ValueError, message="Format specifier must have a set width"):
        reverse_format(pattern, resolved)

    pattern = '{band!s}'
    resolved = '1'
    with pytest.raises(ValueError, message="Conversion not allowed. Found on band"):
        reverse_format(pattern, resolved)
