import logging
from .base import BaseAuth
import uuid

logger = logging.getLogger('intake')


class SecretAuth(BaseAuth):
    """A very simple auth mechanism using a shared secret

    Parameters
    ----------
    secret: str
        The string that must be matched in the requests. If None, a random UUID
        is generated and logged.
    key: str
        Header entry in which to seek the secret
    """

    def __init__(self, secret=None, key='intake-secret'):
        if secret is None:
            secret = uuid.uuid1().hex
            logger.info('Random server secret: %s' % secret)
        self.secret = secret
        self.key = key

    def allow_connect(self, header):
        try:
            return header.get(self.key, '') == self.secret
        except:
            return False

    def allow_access(self, header, source):
        try:
            return header.get(self.key, '') == self.secret
        except:
            return False
