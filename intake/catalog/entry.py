class CatalogEntry(object):
    def __init__(self):
        self._default_source = None

    def __repr__(self):
        return repr(self.describe())

    def describe(self):
        raise NotImplementedError

    def describe_open(self, **user_parameters):
        raise NotImplementedError

    def get(self, **user_parameters):
        raise NotImplementedError

    # Convenience methods for configuring source and automatically
    # creating a default source when needed.
    def __call__(self, **kwargs):
        return self.get(**kwargs)

    def _get_default_source(self):
        if self._default_source is not None:
            return self._default_source
        else:
            self._default_source = self.get()
            return self._default_source

    def __getattr__(self, attr):
        return getattr(self._get_default_source(), attr)
