import os
import pytest
from intake.catalog.local import LocalCatalogEntry, UserParameter
from intake.source.base import DataSource


class NoSource(DataSource):

    def __init__(self, **kwargs):
        self.kwargs = kwargs


driver = 'intake.catalog.tests.test_parameters.NoSource'


def test_simplest():
    e = LocalCatalogEntry('', '', driver, args={'arg1': 1})
    s = e()
    assert s.kwargs['arg1'] == 1


def test_is_cached():
    e = LocalCatalogEntry('', '', driver, args={'arg1': 1})
    s1 = e._get_default_source()
    s2 = e._get_default_source()
    assert s1 is s2


def test_parameter_default():
    up = UserParameter('name', default='oi')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up])
    s = e()
    assert s.kwargs['arg1'] == 'oi'


def test_maybe_default_from_env():
    # maybe fill in parameter default from the env, depending on getenv
    up = UserParameter('name', default='env(INTAKE_TEST_VAR)')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up], getenv=False)
    s = e()
    assert s.kwargs['arg1'] == 'env(INTAKE_TEST_VAR)'

    os.environ['INTAKE_TEST_VAR'] = 'oi'
    s = e()
    assert s.kwargs['arg1'] == 'env(INTAKE_TEST_VAR)'

    up = UserParameter('name', default='env(INTAKE_TEST_VAR)')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up], getenv=True)
    s = e()
    assert s.kwargs['arg1'] == 'oi'

    del os.environ['INTAKE_TEST_VAR']

    s = e()
    assert s.kwargs['arg1'] == ''


def test_up_override_and_render():
    up = UserParameter('name', default='env(INTAKE_TEST_VAR)')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up], getenv=False)
    s = e(name='other')
    assert s.kwargs['arg1'] == 'other'


def test_user_explicit_override():
    up = UserParameter('name', default='env(INTAKE_TEST_VAR)')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up], getenv=False)
    # override wins over up
    s = e(arg1='other')
    assert s.kwargs['arg1'] == 'other'


def test_auto_env_expansion():
    os.environ['INTAKE_TEST_VAR'] = 'oi'
    e = LocalCatalogEntry('', '', driver,
                          args={'arg1': "{{env(INTAKE_TEST_VAR)}}"},
                          parameters=[], getenv=False)
    s = e()

    # when getenv is False, you pass through the text
    assert s.kwargs['arg1'] == '{{env(INTAKE_TEST_VAR)}}'

    e = LocalCatalogEntry('', '', driver,
                          args={'arg1': "{{env(INTAKE_TEST_VAR)}}"},
                          parameters=[], getenv=True)
    s = e()
    assert s.kwargs['arg1'] == 'oi'

    # same, but with quoted environment name
    e = LocalCatalogEntry('', '', driver,
                          args={'arg1': '{{env("INTAKE_TEST_VAR")}}'},
                          parameters=[], getenv=True)
    s = e()
    assert s.kwargs['arg1'] == 'oi'

    del os.environ['INTAKE_TEST_VAR']

    s = e()
    assert s.kwargs['arg1'] == ''

    e = LocalCatalogEntry('', '', driver,
                          args={'arg1': "{{env(INTAKE_TEST_VAR)}}"},
                          parameters=[], getenv=False)
    s = e()
    assert s.kwargs['arg1'] == '{{env(INTAKE_TEST_VAR)}}'


def test_validate_up():
    up = UserParameter('name', default=1, type='int')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up], getenv=False)
    s = e()  # OK
    assert s.kwargs['arg1'] == '1'
    with pytest.raises(ValueError):
        e(name='oi')

    up = UserParameter('name', type='int')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up], getenv=False)
    s = e()  # OK
    # arg1 is a string: real int gets rendered by jinja
    assert s.kwargs['arg1'] == '0'  # default default for int
    s = e(arg1='something')
    assert s.kwargs['arg1'] == 'something'


def test_validate_par():
    up = UserParameter('arg1', type='int')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "oi"},
                          parameters=[up], getenv=False)
    with pytest.raises(ValueError):
        e()
    e = LocalCatalogEntry('', '', driver, args={'arg1': 1},
                          parameters=[up], getenv=False)
    e()  # OK

    e = LocalCatalogEntry('', '', driver, args={'arg1': "1"},
                          parameters=[up], getenv=False)
    s = e()  # OK
    assert s.kwargs['arg1'] == 1  # a number, not str


def test_explicit_overrides():
    e = LocalCatalogEntry('', '', driver, args={'arg1': "oi"})
    s = e(arg1='hi')
    assert s.kwargs['arg1'] == 'hi'

    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"})
    s = e(name='hi')
    assert s.kwargs['arg1'] == 'hi'

    os.environ['INTAKE_TEST_VAR'] = 'another'
    e = LocalCatalogEntry('', '', driver, args={'arg1': "oi"}, getenv=True)
    s = e(arg1='{{env(INTAKE_TEST_VAR)}}')
    assert s.kwargs['arg1'] == 'another'

    up = UserParameter('arg1', type='int')
    e = LocalCatalogEntry('', '', driver, args={'arg1': 1},
                          parameters=[up])
    with pytest.raises(ValueError):
        e(arg1='oi')

    s = e(arg1='1')
    assert s.kwargs['arg1'] == 1


def test_extra_arg():
    e = LocalCatalogEntry('', '', driver, args={'arg1': "oi"})
    s = e(arg2='extra')
    assert s.kwargs['arg2'] == 'extra'


def test_unknown():
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"})
    s = e()
    assert s.kwargs['arg1'] == ""

    # parameter has no default
    up = UserParameter('name')
    e = LocalCatalogEntry('', '', driver, args={'arg1': "{{name}}"},
                          parameters=[up])
    s = e()
    assert s.kwargs['arg1'] == ""
