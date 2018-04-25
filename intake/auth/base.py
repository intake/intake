

class BasicAuth(object):
    """Base class for authorization

    This basic class allows all access.
    """

    def __init__(self, *args):
        self.args = args

    def allow_connect(self, header):
        """Is the requests header given allowed to talk to the server"""
        return True

    def allow_access(self, header, source):
        """Is the given HTTP header allowed to access given data source"""
        return True
