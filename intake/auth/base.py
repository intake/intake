#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from ..utils import DictSerialiseMixin


class AuthenticationFailure(Exception):
    pass


class BaseAuth(DictSerialiseMixin):
    """Base class for authorization

    Subclass this and override the methods to implement a new type of auth.

    This basic class allows all access.
    """

    def __init__(self, *args):
        self.args = args

    def allow_connect(self, header):
        """Is the requests header given allowed to talk to the server

        Parameters
        ----------
        header: dict
            The HTTP header from the incoming request
        """
        return True

    def allow_access(self, header, source, catalog):
        """Is the given HTTP header allowed to access given data source

        Parameters
        ----------
        header: dict
            The HTTP header from the incoming request
        source: CatalogEntry
            The data source the user wants to access.
        catalog: Catalog
            The catalog object containing this data source.
        """
        return True

    def get_case_insensitive(self, dictionary, key, default=None):
        """Case-insensitive search of a dictionary for key.

        Returns the value if key match is found, otherwise default.
        """
        lower_key = key.lower()
        for k, v in dictionary.items():
            if lower_key == k.lower():
                return v
        else:
            return default


class BaseClientAuth(object):
    """Base class for client-side setting of authorization headers

    This basic class adds no headers to remote catalog reqests
    """

    def __init__(self, *args):
        self.args = args

    def __dask_tokenize__(self):
        return hash(self)

    @property
    def _tok(self):
        from dask.base import tokenize
        return tokenize({'cls': type(self).__name__, 'args': self.args})

    def __hash__(self):
        return int(self._tok, 16)

    def get_headers(self):
        """Returns a dictionary of HTTP headers for the remote catalog request.
        """
        return {}
