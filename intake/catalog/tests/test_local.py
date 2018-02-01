import os.path
import tempfile
import shutil
import time

import pytest
import yaml

from .util import assert_items_equal
from ..utils import PermissionsError
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


def test_expand_template_env_str():
    template = 'foo {{ env("USER") }} baz'
    ts = local.TemplateStr(template)
    context = local.TemplateContext('')

    assert repr(ts) == 'TemplateStr(\'foo {{ env("USER") }} baz\')'
    assert str(ts) == template
    assert ts.expand(context) == 'foo {} baz'.format(os.environ['USER'])
    assert ts == local.TemplateStr(template)
    assert ts != template
    assert ts != local.TemplateStr('other')

    context = local.TemplateContext('', env_access=False)
    with pytest.raises(PermissionsError):
        assert ts.expand(context) == 'foo {} baz'.format(os.environ['USER'])


def test_expand_template_shell_str():
    template = 'foo {{ shell("echo bar")[0] }} baz'
    ts = local.TemplateStr(template)
    context = local.TemplateContext('')

    assert repr(ts) == 'TemplateStr(\'foo {{ shell("echo bar")[0] }} baz\')'
    assert str(ts) == template
    assert ts.expand(context) == 'foo bar baz'
    assert ts == local.TemplateStr(template)
    assert ts != template
    assert ts != local.TemplateStr('other')

    context = local.TemplateContext('', shell_access=False)
    with pytest.raises(PermissionsError):
        assert ts.expand(context) == 'foo bar baz'


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

    assert_items_equal(list(union_cat), ['entry1', 'entry1_part', 'use_example1'])

    assert union_cat.entry1_part.describe() == {
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

    desc_open = union_cat.entry1_part.describe_open()
    assert desc_open['args']['urlpath'].endswith('entry1_1.csv')
    del desc_open['args']['urlpath']  # Full path will be system dependent
    assert desc_open == {
        'args': {'metadata': {'bar': [2, 4, 6], 'foo': 'baz'}},
        'description': 'entry1 part',
        'direct_access': 'allow',
        'metadata': {'bar': [2, 4, 6], 'foo': 'baz'},
        'plugin': 'csv'
    }

    # Implied creation of data source
    assert union_cat.entry1.container == 'dataframe'
    assert union_cat.entry1.metadata == dict(foo='bar', bar=[1, 2, 3])

    # Use default parameters in explict creation of data source
    assert union_cat.entry1_part().container == 'dataframe'
    # Specify parameters in creation of data source
    assert union_cat.entry1_part(part='2').container == 'dataframe'


def test_empty_catalog_file():
    empty = os.path.join(os.path.dirname(__file__), '..', 'empty.yml')
    cat = Catalog(empty)
    assert list(cat) == []


def test_duplicate_data_sources():
    path = os.path.dirname(__file__)
    uri = os.path.join(path, 'catalog_dup_sources.yml')

    with pytest.raises(local.ValidationError) as except_info:
        c = Catalog(uri)
    assert 'already exists' in str(except_info.value)


def test_duplicate_parameters():
    path = os.path.dirname(__file__)
    uri = os.path.join(path, 'catalog_dup_parameters.yml')

    with pytest.raises(local.ValidationError) as except_info:
        c = Catalog(uri)
    assert 'already exists' in str(except_info.value)


@pytest.fixture
def temp_catalog_file():
    path = tempfile.mkdtemp()
    catalog_file = os.path.join(path, 'catalog.yaml')
    with open(catalog_file, 'w') as f:
        f.write('''
sources:
  - name: a
    driver: csv
    args:
      urlpath: /not/a/file
  - name: b
    driver: csv
    args:
      urlpath: /not/a/file
        ''')

    yield catalog_file

    shutil.rmtree(path)


def test_catalog_file_removal(temp_catalog_file):
    cat_dir = os.path.dirname(temp_catalog_file)
    cat = Catalog(cat_dir) 
    assert set(cat) == set(['a', 'b'])

    os.remove(temp_catalog_file)
    time.sleep(1.5) # wait for catalog refresh
    assert set(cat) == set()
