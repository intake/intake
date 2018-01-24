import datetime
import os.path

import pytest
import yaml

import pandas

from .util import assert_items_equal
from intake.catalog import Catalog, local


def test_template_str():
    template = 'foo {{ x }} baz'
    ts = local.TemplateStr(template)

    assert repr(ts) == 'TemplateStr(\'foo {{ x }} baz\')'
    assert str(ts) == template
    assert ts.expand(dict(x='bar')) == 'foo bar baz'
    assert ts == local.TemplateStr(template)
    assert ts != template
    assert ts != local.TemplateStr('other')


EXAMPLE_YAML = '''
taxi_data:
  description: entry1
  driver: csv
  args: # passed to the open() method
    urlpath: !template entry1_{{ x }}.csv
    other: !template "entry2_{{ x }}.csv"
'''


def test_yaml_with_templates():
    # Exercise round-trip
    round_trip_yaml = yaml.dump(yaml.safe_load(EXAMPLE_YAML))

    assert "!template 'entry1_{{ x }}.csv'" in round_trip_yaml
    assert "!template 'entry2_{{ x }}.csv'" in round_trip_yaml


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return Catalog(os.path.join(path, 'catalog1.yml'))


def test_local_catalog(catalog1):
    assert_items_equal(list(catalog1), ['use_example1', 'entry1', 'entry1_part'])
    assert catalog1['entry1'].describe() == {
        'container': 'dataframe',
        'direct_access': 'forbid',
        'user_parameters': [],
        'description': 'entry1 full'
    }
    assert catalog1['entry1_part'].describe() == {
        'container': 'dataframe',
        'user_parameters': [
            {
                'name': 'part',
                'description': 'part of filename',
                'default': '1',
                'type': 'str',
                'allowed': ['1', '2'],
            }
        ],
        'description': 'entry1 part',
        'direct_access': 'allow'
    }
    assert catalog1['entry1'].get().container == 'dataframe'
    assert catalog1['entry1'].get().metadata == dict(foo='bar', bar=[1, 2, 3])

    # Use default parameters
    assert catalog1['entry1_part'].get().container == 'dataframe'
    # Specify parameters
    assert catalog1['entry1_part'].get(part='2').container == 'dataframe'


def test_source_plugin_config(catalog1):
    assert_items_equal(list(catalog1.plugins), ['example1', 'example2'])


def test_use_source_plugin_from_config(catalog1):
    catalog1['use_example1'].get()


@pytest.mark.parametrize("dtype,given,expected", [
    ("bool", "true", True),
    ("bool", 0, False),
    ("datetime", datetime.datetime(2018, 1, 1, 0, 34, 0), pandas.Timestamp(2018, 1, 1, 0, 34, 0)),
    ("datetime", "2018-01-01 12:34AM", pandas.Timestamp(2018, 1, 1, 0, 34, 0)),
    ("datetime", 1234567890000000000, pandas.Timestamp(2009, 2, 13, 23, 31, 30)),
    ("float", "3.14", 3.14),
    ("int", "1", 1),
    ("list", (3, 4), [3, 4]),
    ("str", None, "None"),
    ("unicode", "foo", u"foo"),
])
def test_user_parameter_coerce_value(dtype, given, expected):
    p = local.UserParameter('a', 'a desc', dtype, given)
    assert p.validate(given) == expected


@pytest.mark.parametrize("given", ["now", "today"])
def test_user_parameter_coerce_special_datetime(given):
    p = local.UserParameter('a', 'a desc', 'datetime', given)
    assert type(p.validate(given)) == pandas.Timestamp


@pytest.mark.parametrize("dtype,given,expected", [
    ("float", "100.0", 100.0),
    ("int", "20", 20),
    ("int", 20.0, 20),
])
def test_user_parameter_coerce_min(dtype, given, expected):
    p = local.UserParameter('a', 'a desc', dtype, expected, min=given)
    assert p.min == expected


@pytest.mark.parametrize("dtype,given,expected", [
    ("float", "100.0", 100.0),
    ("int", "20", 20),
    ("int", 20.0, 20),
])
def test_user_parameter_coerce_max(dtype, given, expected):
    p = local.UserParameter('a', 'a desc', dtype, expected, max=given)
    assert p.max == expected


@pytest.mark.parametrize("dtype,given,expected", [
    ("float", [50, "100.0", 150.0], [50.0, 100.0, 150.0]),
    ("int", [1, "2", 3.0], [1, 2, 3]),
])
def test_user_parameter_coerce_allowed(dtype, given, expected):
    p = local.UserParameter('a', 'a desc', dtype, expected[0], allowed=given)
    assert p.allowed == expected


def test_user_parameter_validation_range():
    p = local.UserParameter('a', 'a desc', 'int', 1, min=0, max=3)

    with pytest.raises(ValueError) as except_info:
        p.validate(-1)
    assert 'less than' in str(except_info.value)

    assert p.validate(0) == 0
    assert p.validate(1) == 1
    assert p.validate(2) == 2
    assert p.validate(3) == 3

    with pytest.raises(ValueError) as except_info:
        p.validate(4)
    assert 'greater than' in str(except_info.value)


def test_user_parameter_validation_allowed():
    p = local.UserParameter('a', 'a desc', 'int', 1, allowed=[1, 2])

    with pytest.raises(ValueError) as except_info:
        p.validate(0)
    assert 'allowed' in str(except_info.value)

    assert p.validate(1) == 1
    assert p.validate(2) == 2

    with pytest.raises(ValueError) as except_info:
        p.validate(3)
    assert 'allowed' in str(except_info.value)


def test_union_catalog():
    path = os.path.dirname(__file__)
    uri1 = os.path.join(path, 'catalog_union_1.yml')
    uri2 = os.path.join(path, 'catalog_union_2.yml')

    union_cat = Catalog([uri1, uri2])

    assert_items_equal(list(union_cat), ['catalog_union_1', 'catalog_union_2'])

    assert union_cat.catalog_union_1.entry1_part.describe() == {
        'container': 'dataframe',
        'user_parameters': [
            {
                'name': 'part',
                'description': 'part of filename',
                'default': '1',
                'type': 'str',
                'allowed': ['1', '2'],
            }
        ],
        'description': 'entry1 part',
        'direct_access': 'allow'
    }

    desc_open = union_cat.catalog_union_1.entry1_part.describe_open()
    assert desc_open['args']['urlpath'].endswith('entry1_1.csv')
    del desc_open['args']['urlpath']  # Full path will be system dependent
    assert desc_open == {
        'args': {'metadata': {'bar': [2, 4, 6], 'foo': 'baz'}},
        'description': 'entry1 part',
        'direct_access': 'allow',
        'metadata': {'bar': [2, 4, 6], 'foo': 'baz'},
        'plugin': 'csv'
    }

    assert union_cat.catalog_union_2.entry1.get().container == 'dataframe'
    assert union_cat.catalog_union_2.entry1.get().metadata == dict(foo='bar', bar=[1, 2, 3])

    # Use default parameters
    assert union_cat.catalog_union_1.entry1_part.get().container == 'dataframe'
    # Specify parameters
    assert union_cat.catalog_union_2.entry1_part.get(part='2').container == 'dataframe'
