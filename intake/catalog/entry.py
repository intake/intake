class CatalogEntry(object):
    """A single item appearing in a catalog

    This is the base class, used by local entries (i.e., read from a YAML file)
    and by remote entries (read from a server).
    """
    def __init__(self, getenv=True, getshell=True):
        self._default_source = None
        self.getenv = getenv
        self.getshell = getshell

    def __repr__(self):
        return repr(self.describe())

    def describe(self):
        """Get a dictionary of attributes of this entry.

        Returns: dict with keys

          container : str 
              kind of container used by this data source
          description : str
              Markdown-friendly description of data source
          direct_access : str
              Mode of remote access: forbid, allow, force
          user_parameters : list[dict]
              List of user parameters defined by this entry

        """
        raise NotImplementedError

    def describe_open(self, **user_parameters):
        """Get a dictionary describing how to open this data source.

        Parameters
        ----------
          user_parameters : dict
              Values for user-configurable parameters for this data source

        Returns: dict with keys
          plugin : str
              Name of data plugin to use
          description : str
              Markdown-friendly description of data source 
          direct_access : str
              Mode of remote access: forbid, allow, force
          metadata : dict
              Dictionary of metadata defined in the catalog for this data source
          args: dict
              Dictionary of keyword arguments for the plugin
        
        """
        raise NotImplementedError

    def get(self, **user_parameters):
        """Open the data source.

        Equivalent to calling the catalog entry like a function.

        Parameters
        ----------
          user_parameters : dict
              Values for user-configurable parameters for this data source

        Returns: DataSource
        """
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
        # TODO: only consider attr not starting with "_"?
        return getattr(self._get_default_source(), attr)

    def __repr__(self):
        return "<Catalog Entry: %s>" % self.name