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
        from ..source.base import PersistMixin
        if persist is not None and persist not in [
                'always', 'never', 'default']:
            raise ValueError('Persist value (%s) not understood' % persist)
        persist = persist or self._pmode
        s = self.get(**kwargs)
        if persist != 'never' and isinstance(s, PersistMixin) and s.has_been_persisted:
            from ..container.persist import store

            s2 = s.get_persisted()
            met = s2.metadata
            if persist == 'always' or not met['ttl']:
                s = s2
            elif met['ttl'] < time.time() - met['timestamp']:
                s = s2
            else:
                s = store.refresh(s2)
        s._entry = self
        s._passed_kwargs = list(kwargs)
        return s

    @property
    def container(self):
        return getattr(self, '_container', None)

    @container.setter
    def container(self, cont):
        # so that .container (which sources always have)  always reflects ._container,
        # which is the variable name for entries.
        self._container = cont

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
        import json
        contents = self.describe()
        display({
            'application/json': json.dumps(contents),
            'text/plain': pretty_describe(contents)
        }, metadata={
            'application/json': {'root': contents["name"]}
        }, raw=True)

    def _yaml(self):
        return {"sources": {self.name: self.describe()}}

    def __iter__(self):
        # If the entry is a catalog, this allows list(cat.entry)
        if self._container == 'catalog':
            return iter(self())
        else:
            raise ValueError('Cannot iterate a catalog entry')

    def __getitem__(self, item):
        """Pass getitem to data source, assuming default parameters

        Also supports multiple items ([.., ..]), in which case the first
        component only will be used to instantiate, and the rest passed on.
        """
        if isinstance(item, tuple):
            if len(item) > 1:
                return self()[item[0]].__getitem__(item[1:])
            else:
                item = item[0]
        return self()[item]

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
