#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------


class CatalogException(Exception):
    """Basic exception for errors raised by catalog"""


class PermissionDenied(CatalogException):
    """Raised when user requests functionality that they do not have permission
    to access.
    """


class ShellPermissionDenied(PermissionDenied):
    """The user does not have permission to execute shell commands."""
    def __init__(self, msg=None):
        if msg is None:
            msg = "Additional permissions needed to execute shell commands."
        super(ShellPermissionDenied, self).__init__(msg)


class EnvironmentPermissionDenied(PermissionDenied):
    """The user does not have permission to read environment variables."""
    def __init__(self, msg=None):
        if msg is None:
            msg = "Additional permissions needed to read environment variables."
        super(EnvironmentPermissionDenied, self).__init__(msg)


class ValidationError(CatalogException):
    """Something's wrong with the catalog spec"""
    def __init__(self, message, errors):
        super(ValidationError, self).__init__(message)
        self.errors = errors


class DuplicateKeyError(ValidationError):
    """Catalog contains key duplications"""
    def __init__(self, context, context_mark, problem, problem_mark):
        line = problem_mark.line
        column = problem_mark.column
        msg = "duplicate key found on line {}, column {}".format(
            line + 1, column + 1)
        super(DuplicateKeyError, self).__init__(msg, [])


class ObsoleteError(ValidationError):
    pass


class ObsoleteParameterError(ObsoleteError):
    def __init__(self):
        msg = """Detected old syntax. See details for upgrade instructions to new syntax:

[old syntax]

parameters:
  - name: abc
    type: str

[new syntax]

parameters:
  abc:
    type: str
"""
        super(ObsoleteParameterError, self).__init__(msg, [])


class ObsoleteDataSourceError(ObsoleteError):
    def __init__(self):
        msg = """Detected old syntax. See details for upgrade instructions to new syntax:

[old syntax]

sources:
  - name: abc
    driver: csv

[new syntax]

sources:
  abc:
    driver: csv
"""
        super(ObsoleteDataSourceError, self).__init__(msg, [])
