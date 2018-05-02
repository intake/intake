
from intake.auth.base import BaseAuth
from intake.auth.secret import SecretAuth
from intake.auth import get_auth_class


def test_get():
    auth = get_auth_class('intake.auth.base.BaseAuth')
    assert isinstance(auth, BaseAuth)
    auth= get_auth_class('intake.auth.secret.SecretAuth')
    assert isinstance(auth, SecretAuth)


def test_basic():
    auth = BaseAuth()
    assert auth.allow_connect(None)
    assert auth.allow_access(None, None)


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
