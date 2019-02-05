#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import datetime
import os.path
import shutil
import tempfile
import time

import pytest

import pandas

from .util import assert_items_equal
from intake import Catalog, open_catalog
from intake.catalog import exceptions, local
from intake.catalog.local import get_dir, UserParameter, LocalCatalogEntry
from intake.utils import make_path_posix


def abspath(filename):
    return make_path_posix(
        os.path.join(os.path.dirname(__file__), filename))


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return Catalog(os.path.join(path, 'catalog1.yml'))


def test_local_catalog(catalog1):
    assert_items_equal(list(catalog1),
                       ['use_example1', 'nested', 'entry1', 'entry1_part',
                        'remote_env', 'local_env', 'text', 'arr'])
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
    md = catalog1['entry1'].get().metadata
    md.pop('catalog_dir')
    assert md == dict(foo='bar', bar=[1, 2, 3], cache=[])

    # Use default parameters
    assert catalog1['entry1_part'].get().container == 'dataframe'
    # Specify parameters
    assert catalog1['entry1_part'].get(part='2').container == 'dataframe'


def test_nested(catalog1):
    assert 'nested' in catalog1
    assert 'entry1' in catalog1.nested.nested()
    assert catalog1.entry1.read().equals(catalog1.nested.nested.entry1.read())
    assert 'nested.nested' not in catalog1.walk(depth=1)
    assert 'nested.nested' in catalog1.walk(depth=2)


def test_getitem(catalog1):
    assert list(catalog1) == list(catalog1['nested']())
    assert list(catalog1) == list(catalog1['nested.nested']())
    assert list(catalog1) == list(catalog1['nested', 'nested']())


def test_source_plugin_config(catalog1):
    from intake import registry
    assert 'example1' in registry
    assert 'example2' in registry


def test_metadata(catalog1):
    assert hasattr(catalog1, 'metadata')
    assert catalog1.metadata['test'] is True


def test_use_source_plugin_from_config(catalog1):
    catalog1['use_example1'].get()


def test_get_dir():
    assert get_dir('s3://path/catalog.yml') == 's3://path'
    assert get_dir('https://example.com/catalog.yml') == 'https://example.com'
    path = 'example/catalog.yml'
    out = get_dir(path)
    assert os.path.isabs(out)
    assert out.endswith('/example/')
    path = '/example/catalog.yml'
    out = get_dir(path)
    # it's ok if the first two chars indicate drive for win (C:)
    assert '/example/' in [out, out[2:]]
    path = 'example'
    out = get_dir(path)
    assert os.path.isabs(out)
    assert not out.endswith('/example')
    assert out.endswith('/')


@pytest.mark.parametrize("dtype,expected", [
    ("bool", False),
    ("datetime", pandas.Timestamp(1970, 1, 1, 0, 0, 0)),
    ("float", 0.0),
    ("int", 0),
    ("list", []),
    ("str", ""),
    ("unicode", u""),
])
def test_user_parameter_default_value(dtype, expected):
    p = local.UserParameter('a', 'a desc', dtype)
    assert p.validate(None) == expected


@pytest.mark.parametrize("dtype,given,expected", [
    ("bool", "true", True),
    ("bool", 0, False),
    ("datetime", datetime.datetime(2018, 1, 1, 0, 34, 0), pandas.Timestamp(2018, 1, 1, 0, 34, 0)),
    ("datetime", "2018-01-01 12:34AM", pandas.Timestamp(2018, 1, 1, 0, 34, 0)),
    ("datetime", 1234567890000000000, pandas.Timestamp(2009, 2, 13, 23, 31, 30)),
    ("float", "3.14", 3.14),
    ("int", "1", 1),
    ("list", (3, 4), [3, 4]),
    ("str", 1, "1"),
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


@pytest.mark.parametrize("filename", [
    "catalog_non_dict",
    "data_source_missing",
    "data_source_name_non_string",
    "data_source_non_dict",
    "data_source_value_non_dict",
    "params_missing_required",
    "params_missing_default",
    "params_name_non_string",
    "params_non_dict",
    "params_value_bad_choice",
    "params_value_bad_type",
    "params_value_non_dict",
    "plugins_non_dict",
    "plugins_source_both",
    "plugins_source_missing",
    "plugins_source_missing_key",
    "plugins_source_non_dict",
    "plugins_source_non_list",
])
def test_parser_validation_error(filename):
    with pytest.raises(exceptions.ValidationError):
        Catalog(abspath(filename + ".yml"))


@pytest.mark.parametrize("filename", [
    "obsolete_data_source_list",
    "obsolete_params_list",
])
def test_parser_obsolete_error(filename):
    with pytest.raises(exceptions.ObsoleteError):
        Catalog(abspath(filename + ".yml"))


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
    desc_open['args']['metadata'].pop('catalog_dir')
    assert 'csv' in str(desc_open.pop('plugin'))
    assert desc_open == {
        'args': {'metadata': {'bar': [2, 4, 6], 'cache': [], 'foo': 'baz'}},
        'description': 'entry1 part',
        'direct_access': 'allow',
        'metadata': {'bar': [2, 4, 6], 'cache': [], 'foo': 'baz'},
    }

    # Implied creation of data source
    assert union_cat.entry1.container == 'dataframe'
    md = union_cat.entry1.metadata
    md.pop('catalog_dir')
    assert md == dict(foo='bar', bar=[1, 2, 3], cache=[])

    # Use default parameters in explict creation of data source
    assert union_cat.entry1_part().container == 'dataframe'
    # Specify parameters in creation of data source
    assert union_cat.entry1_part(part='2').container == 'dataframe'


def test_empty_catalog():
    cat = Catalog()
    assert list(cat) == []


def test_nonexistent_error():
    with pytest.raises(IOError):
        local.YAMLFileCatalog('nonexistent')


def test_duplicate_data_sources():
    path = os.path.dirname(__file__)
    uri = os.path.join(path, 'catalog_dup_sources.yml')

    with pytest.raises(exceptions.DuplicateKeyError):
        Catalog(uri)


def test_duplicate_parameters():
    path = os.path.dirname(__file__)
    uri = os.path.join(path, 'catalog_dup_parameters.yml')

    with pytest.raises(exceptions.DuplicateKeyError):
        Catalog(uri)


@pytest.fixture
def temp_catalog_file():
    path = tempfile.mkdtemp()
    catalog_file = os.path.join(path, 'catalog.yaml')
    with open(catalog_file, 'w') as f:
        f.write('''
sources:
  a:
    driver: csv
    args:
      urlpath: /not/a/file
  b:
    driver: csv
    args:
      urlpath: /not/a/file
        ''')

    yield catalog_file

    shutil.rmtree(path)


def test_catalog_file_removal(temp_catalog_file):
    cat_dir = os.path.dirname(temp_catalog_file)
    cat = Catalog(cat_dir + '/*')
    assert set(cat) == {'a', 'b'}

    os.remove(temp_catalog_file)
    time.sleep(1.5)  # wait for catalog refresh
    assert set(cat) == set()


def test_flatten_duplicate_error():
    path = tempfile.mkdtemp()
    f1 = os.path.join(path, 'catalog.yaml')
    path = tempfile.mkdtemp()
    f2 = os.path.join(path, 'catalog.yaml')
    for f in [f1, f2]:
        with open(f, 'w') as fo:
            fo.write("""
        sources:
          a:
            driver: csv
            args:
              urlpath: /not/a/file
        """)
    with pytest.raises(ValueError):
        open_catalog([f1, f2])


def test_multi_cat_names():
    fn = abspath("catalog_union*.yml")
    cat = open_catalog(fn)
    assert cat.name == fn
    assert fn in repr(cat)

    fn1 = abspath("catalog_union_1.yml")
    fn2 = abspath("catalog_union_2.yml")
    cat = open_catalog([fn1, fn2])
    assert cat.name == '2 files'


def test_default_expansions():
    try:
        os.environ['INTAKE_INT_TEST'] = '1'
        par = UserParameter('', '', 'int', default='env(INTAKE_INT_TEST)')
        par.expand_defaults()
        assert par.expanded_default == 1
    finally:
        del os.environ['INTAKE_INT_TEST']

    par = UserParameter('', '', 'str', default='env(USER)')
    par.expand_defaults(getenv=False)
    assert par.expanded_default == 'env(USER)'
    par.expand_defaults()
    assert par.expanded_default == os.getenv('USER', '')

    par = UserParameter('', '', 'str', default='client_env(USER)')
    par.expand_defaults()
    assert par.expanded_default == 'client_env(USER)'
    par.expand_defaults(client=True)
    assert par.expanded_default == os.getenv('USER', '')

    par = UserParameter('', '', 'str', default='shell(echo success)')
    par.expand_defaults(getshell=False)
    assert par.expanded_default == 'shell(echo success)'
    par.expand_defaults()
    assert par.expanded_default == 'success'

    par = UserParameter('', '', 'str', default='client_shell(echo success)')
    par.expand_defaults(client=True)
    assert par.expanded_default == 'success'

    par = UserParameter('', '', 'int', default=1)
    par.expand_defaults()  # no error from string ops


def test_remote_cat(http_server):
    url = http_server + 'catalog1.yml'
    cat = Catalog(url)
    assert 'entry1' in cat
    assert cat.entry1.describe()


def test_multi_plugins():
    from intake.source.csv import CSVSource
    fn = abspath('multi_plugins.yaml')
    cat = open_catalog(fn)
    s = cat.tables0()
    assert isinstance(s, CSVSource)

    s = cat.tables1()
    assert isinstance(s, CSVSource)

    s = cat.tables2()
    assert isinstance(s, CSVSource)

    s = cat.tables3()
    assert isinstance(s, CSVSource)
    assert s._csv_kwargs == {}

    s = cat.tables3(plugin='myplug')
    assert isinstance(s, CSVSource)
    assert s._csv_kwargs == {}

    s = cat.tables3(plugin='myplug2')
    assert isinstance(s, CSVSource)
    assert s._csv_kwargs is True

    with pytest.raises(ValueError):
        cat.tables4()
    with pytest.raises(ValueError):
        cat.tables4(plugin='myplug')
    with pytest.raises(ValueError):
        cat.tables4(plugin='myplug2')

    s = cat.tables5()
    assert isinstance(s, CSVSource)

    with pytest.raises(ValueError):
        cat.tables5(plugin='myplug')

    fn = abspath('multi_plugins2.yaml')
    with pytest.raises(ValueError):
        open_catalog(fn)


def test_no_plugins():
    fn = abspath('multi_plugins.yaml')
    cat = open_catalog(fn)
    s = cat.tables6
    with pytest.raises(ValueError):
        s()
    s = cat.tables7
    with pytest.raises(ValueError):
        s()


def test_getitem_and_getattr():
    fn = abspath('multi_plugins.yaml')
    catalog = open_catalog(fn)
    catalog['tables0']
    with pytest.raises(KeyError):
        catalog['doesnotexist']
    with pytest.raises(KeyError):
        catalog['_doesnotexist']
    with pytest.raises(KeyError):
        # This exists as an *attribute* but not as an item.
        catalog['metadata']
    catalog.tables0  # alias to catalog['tables0']
    catalog.metadata  # a normal attribute
    with pytest.raises(AttributeError):
        catalog.doesnotexit
    with pytest.raises(AttributeError):
        catalog._doesnotexit
    assert catalog.tables0 is catalog['tables0']
    assert isinstance(catalog.tables0, LocalCatalogEntry)
    assert isinstance(catalog.metadata, (dict, type(None)))
