
from intake.auth.base import BaseAuth, BaseClientAuth
from intake.auth.secret import SecretAuth,SecretClientAuth
from intake.auth import get_auth_class


def test_get():
    auth = get_auth_class('intake.auth.base.BaseAuth')
    assert isinstance(auth, BaseAuth)
    auth= get_auth_class('intake.auth.secret.SecretAuth')
    assert isinstance(auth, SecretAuth)


def test_base():
    auth = BaseAuth()
    assert auth.allow_connect(None)
    assert auth.allow_access(None, None)


def test_base_client():
    auth = BaseClientAuth()
    assert auth.get_headers() == {}


def test_secret():
    secret = 'test-secret'
    auth = SecretAuth(secret=secret)
    assert not auth.allow_connect({})
    assert not auth.allow_connect({'intake-secret': ''})
    assert not auth.allow_connect({'intake-secret': None})
    assert not auth.allow_connect({'intake-secret': 'wrong'})
    assert auth.allow_connect({'intake-secret': secret})

    assert not auth.allow_access({'intake-secret': 'wrong'}, None)
    assert auth.allow_access({'intake-secret': secret}, None)

    auth = SecretAuth(secret=secret, key='another_header')
    assert not auth.allow_connect({'intake-secret': secret})
    assert auth.allow_connect({'another_header': secret})


def test_secret_client():
    secret = 'test-secret'
    auth = SecretClientAuth(secret=secret)
    assert auth.get_headers() == { 'intake-secret': secret}

    auth = SecretClientAuth(secret=secret, key='another_header')
    assert auth.get_headers() == { 'another_header': secret}
