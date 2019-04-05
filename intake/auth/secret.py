#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import logging
from .base import BaseAuth, BaseClientAuth
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
            return self.get_case_insensitive(header, self.key, '') \
                        == self.secret
        except:
            return False

    def allow_access(self, header, source, catalog):
        try:
            return self.get_case_insensitive(header, self.key, '') \
                        == self.secret
        except:
            return False


class SecretClientAuth(BaseClientAuth):
    """Matching client auth plugin to SecretAuth

    Parameters
    ----------
    secret: str
        The string that must be included requests.
    key: str
        HTTP Header key for the shared secret
    """

    def __init__(self, secret, key='intake-secret'):
        self.secret = secret
        self.key = key
        super(SecretClientAuth, self).__init__()

    def get_headers(self):
        return {self.key: self.secret}
