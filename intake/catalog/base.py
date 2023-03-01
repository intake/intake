# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import keyword
import logging
import re
import time

from ..source.base import DataSource, DataSourceBase, NoEntry
from .utils import reload_on_change

logger = logging.getLogger("intake")


class Catalog(DataSource):
    """Manages a hierarchy of data sources as a collective unit.

    A catalog is a set of available data sources for an individual
    entity (remote server, local  file, or a local
    directory of files). This can be expanded to include a
    collection of subcatalogs, which are then managed as a single unit.

    A catalog is created with a single URI or a group of URIs. A URI can
    either be a URL or a file path.

    Each catalog in the hierarchy is responsible for caching the most recent
    refresh time to prevent overeager queries.

    Attributes
    ----------
    metadata : dict
        Arbitrary information to carry along with the data source specs.
    """

    # emulate a DataSource
    container = "catalog"
    name = "catalog"
    auth = None

    def __init__(
        self,
        entries=None,
        name=None,
        description=None,
        metadata=None,
        ttl=60,
        getenv=True,
        getshell=True,
        persist_mode="default",
        storage_options=None,
        user_parameters=None,
    ):
        """
        Parameters
        ----------
        entries : dict, optional
            Mapping of {name: entry}
        name : str, optional
            Unique identifier for catalog. This takes precedence over whatever
            is stated in the cat file itself. Defaults to None.
        description : str, optional
            Description of the catalog. This takes precedence over whatever
            is stated in the cat file itself. Defaults to None.
        metadata: dict
            Additional information about this data
        ttl : float, optional
            Lifespan (time to live) of cached modification time. Units are in
            seconds. Defaults to 1.
        getenv: bool
            Can parameter default fields take values from the environment
        getshell: bool
            Can parameter default fields run shell commands
        persist_mode: ['always', 'default', 'never']
            Defines the use of persisted sources: if 'always', will use a
            persisted version of a data source, if it exists, if 'never' will
            always use the original source. If 'default', persisted sources
            will be used if they have not expired, and re-persisted and used
            if they have.
        storage_options : dict
            If using a URL beginning with 'intake://' (remote Intake server),
            parameters to pass to requests when issuing http commands; otherwise
            parameters to pass to remote backend file-system. Ignored for
            normal local files.
        """
        super(Catalog, self).__init__()
        self.name = name
        self.description = description
        self.metadata = metadata or {}
        self.ttl = ttl
        self.getenv = getenv
        self.getshell = getshell
        self.storage_options = storage_options
        if isinstance(user_parameters, dict) and user_parameters:
            from .local import UserParameter

            self.user_parameters = {name: (UserParameter(name=name, **up) if isinstance(up, dict) else up) for name, up in user_parameters.items()}
        elif isinstance(user_parameters, (list, tuple)):
            self.user_parameters = {up["name"]: up for up in user_parameters}
        else:
            self.user_parameters = {}
        if persist_mode not in ["always", "never", "default"]:
            # should be True, False, None ?
            raise ValueError("Persist mode (%s) not understood" % persist_mode)
        self.pmode = persist_mode

        if entries and isinstance(entries, str):
            raise ValueError(
                "The class intake.Catalog does not accept a string for "
                "`entries`\n"
                "Did you mean to use `intake.open_catalog`? Note that in "
                "versions of intake <=0.5.4 `intake.Catalog` was an "
                "alias for `intake.open_catalog`. It is now the intake base "
                "Catalog class."
            )
        self.updated = time.time()
        self._entries = entries if entries is not None else self._make_entries_container()
        self.force_reload()

    @classmethod
    def from_dict(cls, entries, **kwargs):
        """
        Create Catalog from the given set of entries

        Parameters
        ----------
        entries : dict-like
            A mapping of name:entry which supports dict-like functionality,
            e.g., is derived from ``collections.abc.Mapping``.
        kwargs : passed on the constructor
            Things like metadata, name; see ``__init__``.

        Returns
        -------
        Catalog instance
        """
        cat = cls(**kwargs)
        cat._entries = entries
        return cat

    @property
    def kwargs(self):
        return dict(name=self.name, ttl=self.ttl)

    def _make_entries_container(self):
        """Subclasses may override this to return some other dict-like.

        See RemoteCatalog below for the motivating example for this hook. This
        is typically useful for large Catalogs backed by dynamic resources such
        as databases.

        The object returned by this method must implement:

        * ``__iter__()`` -> an iterator of entry names
        * ``__getitem__(key)`` -> an Entry
        * ``items()`` -> an iterator of ``(key, Entry)`` pairs

        For best performance the object should also implement:

        * ``__len__()`` -> int
        * ``__contains__(key)`` -> boolean

        In ``__len__`` or ``__contains__`` are not implemented, intake will
        fall back on iterating through the entire catalog to compute its length
        or check for containment, which may be expensive on large catalogs.
        """
        return {}

    def _load(self):
        """Override this: load catalog entries"""
        pass

    def force_reload(self):
        """Imperative reload data now"""
        self.updated = time.time()
        self._load()

    def reload(self):
        """Reload catalog if sufficient time has passed"""
        if (self.ttl is not None) and (time.time() - self.updated > self.ttl):
            self.force_reload()

    @property
    def version(self):
        # default version for pre-v1 files
        return self.metadata.get("version", 1)

    @reload_on_change
    def search(self, text, depth=2):
        import copy

        words = text.lower().split()
        entries = {k: copy.copy(v) for k, v in self.walk(depth=depth).items() if any(word in str(v.describe().values()).lower() for word in words)}
        cat = Catalog.from_dict(
            entries,
            name=self.name + "_search",
            ttl=self.ttl,
            getenv=self.getenv,
            getshell=self.getshell,
            metadata=(self.metadata or {}).copy(),
            storage_options=self.storage_options,
            user_parameters=self.user_parameters.copy(),
        )
        cat.metadata["search"] = {"text": text, "upstream": self.name}
        cat.cat = self
        for e in entries.values():
            e._catalog = cat
        return cat

    def filter(self, func):
        """
        Create a Catalog of a subset of entries based on a condition

        .. warning ::

           This function operates on CatalogEntry objects not DataSource
           objects.

        .. note ::

            Note that, whatever specific class this is performed on,
            the return instance is a Catalog. The entries are passed
            unmodified, so they will still reference the original catalog
            instance and include its details such as directory,.

        Parameters
        ----------
        func : function
            This should take a CatalogEntry and return True or False. Those
            items returning True will be included in the new Catalog, with the
            same entry names

        Returns
        -------
        Catalog
           New catalog with Entries that still refer to their parents
        """
        return Catalog.from_dict({key: entry for key, entry in self._entries.items() if func(entry)})

    @reload_on_change
    def walk(self, sofar=None, prefix=None, depth=2):
        """Get all entries in this catalog and sub-catalogs

        Parameters
        ----------
        sofar: dict or None
            Within recursion, use this dict for output
        prefix: list of str or None
            Names of levels already visited
        depth: int
            Number of levels to descend; needed to truncate circular references
            and for cleaner output

        Returns
        -------
        Dict where the keys are the entry names in dotted syntax, and the
        values are entry instances.
        """
        out = sofar if sofar is not None else {}
        prefix = [] if prefix is None else prefix
        for name, item in self._entries.items():
            if item._container == "catalog" and depth > 1:
                # recurse with default open parameters
                try:
                    item().walk(out, prefix + [name], depth - 1)
                except Exception as e:
                    print(e)
                    pass  # ignore inability to descend
            n = ".".join(prefix + [name])
            out[n] = item
        return out

    def items(self):
        """Get an iterator over (key, source) tuples for the catalog entries."""
        for name, entry in self._get_entries().items():
            yield name, entry()

    def values(self):
        """Get an iterator over the sources for catalog entries."""
        for entry in self._get_entries().values():
            yield entry()

    def serialize(self):
        """
        Produce YAML version of this catalog.

        Note that this is not the same as ``.yaml()``, which produces a YAML
        block referring to this catalog.
        """
        import yaml

        output = {"metadata": self.metadata, "sources": {}, "name": self.name, "description": self.description}
        for key, entry in self._entries.items():
            kw = entry._captured_init_kwargs.copy()
            kw.pop("catalog", None)
            kw["parameters"] = {k.name: k.__getstate__()["kwargs"] for k in kw.get("parameters", [])}
            try:
                if issubclass(kw["driver"], DataSourceBase):
                    kw["driver"] = ".".join([kw["driver"].__module__, kw["driver"].__name__])
            except TypeError:
                pass  # ignore exception for a string input
            output["sources"][key] = kw
        return yaml.dump(output)

    def save(self, url, storage_options=None):
        """
        Output this catalog to a file as YAML

        Parameters
        ----------
        url : str
            Location to save to, perhaps remote
        storage_options : dict
            Extra arguments for the file-system
        """
        from fsspec import open_files

        with open_files([url], **(storage_options or {}), mode="wt")[0] as f:
            f.write(self.serialize())

    @reload_on_change
    def _get_entry(self, name):
        entry = self._entries[name]
        entry._catalog = self
        entry._pmode = self.pmode

        up_names = set((up["name"] if isinstance(up, dict) else up.name) for up in entry._user_parameters)
        ups = [up for name, up in self.user_parameters.items() if name not in up_names]
        entry._user_parameters = ups + (entry._user_parameters or [])
        return entry()

    def configure_new(self, **kwargs):
        from .local import UserParameter

        ups = {}
        for k, v in kwargs.copy().items():
            for up in self.user_parameters.values():
                if isinstance(up, dict):
                    if k == up["name"]:
                        kw = up.copy()
                        kw["default"] = v
                        ups[k] = UserParameter(**kw)
                        kwargs.pop(k)
                else:
                    if k == up.name:
                        kw = up._captured_init_kwargs.copy()
                        kw["default"] = v
                        kw["name"] = k
                        ups[k] = UserParameter(**kw)
                        kwargs.pop(k)

        new = super().configure_new(**kwargs)
        new.user_parameters.update(ups)
        return new

    __call__ = get = configure_new

    @reload_on_change
    def _get_entries(self):
        return self._entries

    def __iter__(self):
        """Return an iterator over catalog entry names."""
        return iter(self._get_entries())

    def keys(self):
        """Entry names in this catalog as an iterator (alias for __iter__)"""
        return iter(self)

    def __len__(self):
        return len(self._get_entries())

    def __contains__(self, key):
        # Avoid iterating through all entries.
        return key in self._get_entries()  # triggers reload_on_change

    def __dir__(self):
        # Include tab-completable entries and normal attributes.
        return [
            entry for entry in self if re.match("[_A-Za-z][_a-zA-Z0-9]*$", entry) and not keyword.iskeyword(entry)  # valid Python identifer
        ] + list(  # not a Python keyword
            self.__dict__.keys()
        )

    def _ipython_key_completions_(self):
        return list(self)

    def __repr__(self):
        return "<Intake catalog: %s>" % self.name

    def __getattr__(self, item):
        # we need this special case here because the (deprecated) entry
        # property on the base class
        if item == "entry":
            raise NoEntry("Source was not made from a catalog entry")
        if not item.startswith("_"):
            # Fall back to __getitem__.
            try:
                return self[item]  # triggers reload_on_change
            except KeyError as e:
                raise AttributeError(item) from e
        raise AttributeError(item)

    def __setitem__(self, key, entry):
        """Add entry to catalog

        This relies on the `_entries` attribute being mutable, which it normally
        is. Note that if a catalog automatically reloads, any entry added here
        may be very transient

        Parameters
        ----------
        key : str
            Key to give the entry in the cat
        entry : CatalogEntry
            The entry to include (could be local, remote)
        """
        self._entries[key] = entry

    def pop(self, key):
        """Remove entry from catalog and return it

        This relies on the `_entries` attribute being mutable, which it normally
        is. Note that if a catalog automatically reloads, any entry removed here
        may soon reappear

        Parameters
        ----------
        key : str
            Key to give the entry in the cat
        """
        return self._entries.pop(key)

    def __getitem__(self, key):
        """Return a catalog entry by name.

        Can also use attribute syntax, like ``cat.entry_name``, or
        item lookup cat['non-python name']. This enables walking through
        nested directories with cat.name1.name2, cat['name1.name2'] *or*
        cat['name1', 'name2']
        """
        if not isinstance(key, list) and key in self:
            # triggers reload_on_change
            s = self._get_entry(key)
            if s.container == "catalog":
                s.name = key
                s.user_parameters.update(self.user_parameters.copy())
                return s
            return s
        if isinstance(key, str) and "." in key:
            key = key.split(".")
        if isinstance(key, list):
            parts = list(key)[:]
            prefix = ""
            while parts:
                bit = parts.pop(0)
                prefix = prefix + ("." if prefix else "") + bit
                if prefix in self._entries:
                    rest = ".".join(parts)
                    try:
                        out = self._entries[prefix][rest]
                        return out()
                    except KeyError:
                        # name conflict like "thing" and "think.oi", where it's
                        # the latter we are after
                        continue
        elif isinstance(key, tuple):
            out = self
            for part in key:
                out = out[part]
            return out()
        raise KeyError(key)

    def discover(self):
        return {"container": "catalog", "shape": None, "dtype": None, "metadata": self.metadata}

    def _close(self):
        # TODO: maybe close all entries?
        pass

    @property
    def gui(self):
        if not hasattr(self, "_gui"):
            from ..interface import output_notebook
            from ..interface.gui import GUI

            output_notebook()
            self._gui = GUI(self)
        else:
            self._gui.visible = True
        return self._gui
