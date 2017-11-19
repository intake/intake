class CatalogBase(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def reload(self):
        self.__init__(*self.args, **self.kwargs)

    def __iter__(self):
        return iter(self._entries)

    def __dir__(self):
        return list(self._entries)

    def __getattr__(self, item):
        return self._entries[item]

    def __getitem__(self, item):
        return self._entries[item]


class CatalogEntry(object):
    def __repr__(self):
        return repr(self.describe())

    def describe(self):
        raise NotImplementedError

    def describe_open(self, **user_parameters):
        raise NotImplementedError

    def get(self, **user_parameters):
        raise NotImplementedError
