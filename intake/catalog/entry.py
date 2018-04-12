class CatalogEntry(object):
    """A single item appearing in a catalog

    This is the base class, used by local entries (i.e., read from a YAML file)
    and by remote entries (read from a server).
    """
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
        if self._default_source is None:
            self._default_source = self.get()
        return self._default_source

    def __getattr__(self, attr):
        return getattr(self._get_default_source(), attr)
