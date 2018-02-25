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
    def __init__(self, message, errors):
        super(ValidationError, self).__init__(message)
        self.errors = errors
