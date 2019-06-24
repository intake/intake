#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import time
from ..utils import DictSerialiseMixin, pretty_describe


class CatalogEntry(DictSerialiseMixin):
    """A single item appearing in a catalog

    This is the base class, used by local entries (i.e., read from a YAML file)
    and by remote entries (read from a server).
    """
    def __init__(self, getenv=True, getshell=True):
        self._default_source = None
        self.getenv = getenv
        self.getshell = getshell
        self._pmode = 'default'

    def describe(self):
        """Get a dictionary of attributes of this entry.

        Returns: dict with keys
          name: str
              The name of the catalog entry.
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

    def get(self, **user_parameters):
        """Open the data source.

        Equivalent to calling the catalog entry like a function.

        Note: ``entry()``, ``entry.attr``, ``entry[item]`` check for persisted
        sources, but directly calling ``.get()`` will always ignore the
        persisted store (equivalent to ``self._pmode=='never'``).

        Parameters
        ----------
          user_parameters : dict
              Values for user-configurable parameters for this data source

        Returns
        -------
        DataSource
        """
        raise NotImplementedError

    def __call__(self, persist=None, **kwargs):
        """Instantiate DataSource with given user arguments

        Parameters
        ----------
        persist: str or None
            Override persistence mode defined in the parent catalog. If not
            None, must be one of ['always', 'never', 'default']. Has no
            effect if the source has never been persisted, use source.persist()
        """
        from ..container.persist import store

        if persist is not None and persist not in [
                'always', 'never', 'default']:
            raise ValueError('Persist value (%s) not understood' % persist)
        persist = persist or self._pmode
        s = self.get(**kwargs)
        if s.has_been_persisted and persist is not 'never':
            s2 = s.get_persisted()
            met = s2.metadata
            if persist is 'always' or not met['ttl']:
                return s2
            if met['ttl'] < time.time() - met['timestamp']:
                return s2
            else:
                return store.refresh(s2)
        return s

    def _get_default_source(self):
        """Instantiate DataSource with default agruments"""
        if self._default_source is None:
            self._default_source = self()
        return self._default_source

    @property
    def has_been_persisted(self, **kwargs):
        """For the source created with the given args, has it been persisted?"""
        return self.get(**kwargs).has_been_persisted

    @property
    def plots(self):
        """List custom associated quick-plots """
        return list(self._metadata.get('plots', {}))

    def _ipython_display_(self):
        """Display the entry as a rich object in an IPython session."""
        from IPython.display import display
        contents = self.describe()
        display({
            'application/json': contents,
            'text/plain': pretty_describe(contents)
        }, metadata={
            'application/json': {'root': contents["name"]}
        }, raw=True)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self._get_default_source(), attr)

    def __dir__(self):
        selflist = {'describe', 'describe_open', 'get', 'gui',
                    'has_been_persisted', 'plots'}
        selflist.update(set(dir(self._get_default_source())))
        return list(sorted(selflist))

    def __iter__(self):
        # If the entry is a catalog, this allows list(cat.entry)
        if self._container == 'catalog':
            return iter(self._get_default_source())
        else:
            raise ValueError('Cannot iterate a catalog entry')

    def __getitem__(self, item):
        """Pass getitem to data source, assuming default parameters

        Also supports multiple items ([.., ..]), in which case the first
        component only will be used to instantiate, and the rest passed on.
        """
        if isinstance(item, tuple):
            if len(item) > 1:
                return self._get_default_source()[item[0]].__getitem__(item[1:])
            else:
                item = item[0]
        return self._get_default_source()[item]

    def __repr__(self):
        return pretty_describe(self.describe())

    @property
    def gui(self):
        if not hasattr(self, '_gui'):
            from .gui import EntryGUI
            self._gui = EntryGUI(source=self, visible=True)
        else:
            self._gui.visible = True
        return self._gui
