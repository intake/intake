import os.path

import pytest
import yaml


from .. import local
from .util import assert_items_equal


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
    return local.LocalCatalog(os.path.join(path, 'catalog1.yml'))


def test_local_catalog(catalog1):
    assert_items_equal(catalog1.list(), ['use_example1', 'entry1', 'entry1_part'])
    assert catalog1.describe('entry1') == {
        'container': 'dataframe',
        'direct_access': 'forbid',
        'user_parameters': [],
        'description': 'entry1 full'
    }
    assert catalog1.describe('entry1_part') == {
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
    assert catalog1.get('entry1').container == 'dataframe'
    assert catalog1.get('entry1').metadata == dict(foo='bar', bar=[1, 2, 3])

    # Use default parameters
    assert catalog1.get('entry1_part').container == 'dataframe'
    # Specify parameters
    assert catalog1.get('entry1_part', part='2').container == 'dataframe'


def test_source_plugin_config(catalog1):
    assert_items_equal(catalog1.source_plugins.keys(), ['example1', 'example2'])


def test_use_source_plugin_from_config(catalog1):
    catalog1.get('use_example1')


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
    cat1 = local.LocalCatalog(os.path.join(path, 'catalog_union_1.yml'))
    cat2 = local.LocalCatalog(os.path.join(path, 'catalog_union_2.yml'))

    with pytest.raises(Exception) as except_info:
        union_cat = local.UnionCatalog([cat1, cat2])
    assert 'duplicate' in str(except_info.value)

    union_cat = local.UnionCatalog([('cat1.', cat1), ('cat2.', cat2)])

    assert_items_equal(union_cat.list(),
                       ['cat1.use_example1', 'cat1.entry1_part',
                        'cat2.entry1', 'cat2.entry1_part'])

    assert union_cat.describe('cat1.entry1_part') == {
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

    desc_open = union_cat.describe_open('cat1.entry1_part')
    assert desc_open['args']['urlpath'].endswith('entry1_1.csv')
    del desc_open['args']['urlpath']  # Full path will be system dependent
    assert desc_open == {
        'args': {'metadata': {'bar': [2, 4, 6], 'foo': 'baz'}},
        'description': 'entry1 part',
        'direct_access': 'allow',
        'metadata': {'bar': [2, 4, 6], 'foo': 'baz'},
        'plugin': 'csv'
    }

    assert union_cat.get('cat2.entry1').container == 'dataframe'
    assert union_cat.get('cat2.entry1').metadata == dict(foo='bar', bar=[1, 2, 3])

    # Use default parameters
    assert union_cat.get('cat1.entry1_part').container == 'dataframe'
    # Specify parameters
    assert union_cat.get('cat2.entry1_part', part='2').container == 'dataframe'
