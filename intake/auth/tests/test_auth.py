#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from intake.auth.base import BaseAuth, BaseClientAuth
from intake.auth.secret import SecretAuth,SecretClientAuth
from intake.utils import remake_instance


def test_get():
    auth = remake_instance('intake.auth.base.BaseAuth')
    assert isinstance(auth, BaseAuth)
    auth = remake_instance('intake.auth.secret.SecretAuth')
    assert isinstance(auth, SecretAuth)


def test_base():
    auth = BaseAuth()
    assert auth.allow_connect(None)
    assert auth.allow_access(None, None, None)


def test_base_client():
    auth = BaseClientAuth()
    assert auth.get_headers() == {}


def test_base_get_case_insensitive():
    auth = BaseAuth()
    d = {'foo': 1, 'BAR': 2}
    assert auth.get_case_insensitive(d, 'foo') == 1
    assert auth.get_case_insensitive(d, 'Foo') == 1
    assert auth.get_case_insensitive(d, 'FOO') == 1

    assert auth.get_case_insensitive(d, 'bar') == 2
    assert auth.get_case_insensitive(d, 'Bar') == 2
    assert auth.get_case_insensitive(d, 'BAR') == 2

    assert auth.get_case_insensitive(d, 'no') is None
    assert auth.get_case_insensitive(d, 'no', '') == ''


def test_secret():
    secret = 'test-secret'
    auth = SecretAuth(secret=secret)
    assert not auth.allow_connect({})
    assert not auth.allow_connect({'intake-secret': ''})
    assert not auth.allow_connect({'intake-secret': None})
    assert not auth.allow_connect({'intake-secret': 'wrong'})
    assert auth.allow_connect({'intake-secret': secret})
    # HTTP headers are not case sensitive, and frequently recapitalized
    assert auth.allow_connect({'Intake-Secret': secret})

    assert not auth.allow_access({'intake-secret': 'wrong'}, None, None)
    assert auth.allow_access({'intake-secret': secret}, None, None)

    auth = SecretAuth(secret=secret, key='another_header')
    assert not auth.allow_connect({'intake-secret': secret})
    assert auth.allow_connect({'another_header': secret})


def test_secret_client():
    secret = 'test-secret'
    auth = SecretClientAuth(secret=secret)
    assert auth.get_headers() == { 'intake-secret': secret}

    auth = SecretClientAuth(secret=secret, key='another_header')
    assert auth.get_headers() == { 'another_header': secret}
