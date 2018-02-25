class PermissionsError(Exception):
    """Raised when user requests functionality that they do not have permission
    to access.
    """
    pass


class ValidationError(Exception):
    def __init__(self, message, errors):
        super(ValidationError, self).__init__(message)
        self.errors = errors
