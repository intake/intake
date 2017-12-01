class CatalogEntry(object):
    def __repr__(self):
        return repr(self.describe())

    def describe(self):
        raise NotImplementedError

    def describe_open(self, **user_parameters):
        raise NotImplementedError

    def get(self, **user_parameters):
        raise NotImplementedError
