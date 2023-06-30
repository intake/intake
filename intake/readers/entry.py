"""Description of the ways to load a data set"""
from __future__ import annotations

from collections.abc import Mapping
from itertools import chain
from typing import Any, Iterable

from intake import import_name
from intake.readers.readers import BaseReader
from intake.readers.user_parameters import BaseUserParameter, set_values
from intake.readers.utils import Tokenizable, merge_dicts


class DataDescription(Tokenizable):
    """Defines some data and a single way to load it, with parameters not yet resolved"""

    def __init__(self, datatype: str, kwargs: dict = None, metadata: dict = None, user_parameters: dict = None):
        self.datatype = datatype
        self.kwargs = kwargs or {}
        self._metadata = metadata or {}
        self._ups = user_parameters or {}

    def __repr__(self):
        part = f"DataDescription {self.datatype} {self.kwargs}"
        if self.user_parameters:
            part += f"\n {self.user_parameters}"
        return part

    def to_data(self, **kwargs):
        cls = import_name(self.datatype)
        kw = self.get_kwargs()
        kw.update(kwargs)
        return cls(**kw)

    def get_kwargs(self, user_parameters: dict[str | BaseUserParameter] | None = None, **kwargs) -> dict[str, Any]:
        """Get set of kwargs for given reader, based on prescription, new args and user parameters

        Here, `user_parameters` is intended to come from the containing catalog. To provide values
        for a user parameter, include it by name in kwargs
        """
        kw = self.kwargs.copy()
        up = self._ups.copy()
        up.update(user_parameters or {})
        kw = set_values(up, kw)
        return kw

    @property
    def user_parameters(self):
        return self._ups


class ReaderDescription(Tokenizable):
    def __init__(
        self,
        data: DataDescription,
        reader: str,
        kwargs: dict[str, Any] | None = None,
        user_parameters: dict[str | BaseUserParameter] | None = None,
        metadata: dict | None = None,
    ):
        self.data = data
        self.reader = reader
        self.kwargs = kwargs or dict[str, Any]()
        self._ups = user_parameters or dict[str | BaseUserParameter]()
        self._metadata = metadata or {}

    def get_kwargs(self, **kwargs) -> dict[str, Any]:
        """Get set of kwargs for given reader, based on prescription, new args and user parameters

        Here, `user_parameters` is intended to come from the containing catalog. To provide values
        for a user parameter, include it by name in kwargs
        """
        kw = self.kwargs.copy()
        kw["data"] = self.data.to_data()
        kw.update(kwargs)
        up = self.data.user_parameters.copy()
        up.update(self._ups)
        kw = set_values(up, kw)
        return kw

    def to_reader(self, **kwargs):
        cls = import_name(self.reader)
        kw = self.get_kwargs(**kwargs)
        return cls(**kw)

    def __call__(self, **kwargs):
        return self.to_reader(**kwargs)

    @property
    def metadata(self):
        # reader has own metadata?
        return self.data.metadata

    def __repr__(self):
        return f"Entry for data {self.data}\nreader: {self.reader}\nkwargs: {self.kwargs}"

    def __add__(self, other: DataDescription | BaseReader):
        """makes a catalog from any two descriptions"""
        if not isinstance(other, (DataDescription, BaseReader)):
            raise TypeError
        if isinstance(other, BaseReader):
            other = other.to_entry()
        return Catalog(entries=(self, other))


class Catalog(Mapping):
    def __init__(
        self,
        entries: Iterable[DataDescription],
        aliases: dict[str, int] | None = None,
        user_parameters: dict[str, BaseUserParameter] | None = None,  # global to the catalog
        metadata: dict | None = None,
    ):
        # TODO: extract out all tokens of data instances and pipeline stages
        self.entries: dict[str, DataDescription] = {e.token: e for e in entries}
        self.aliases = aliases or {}  # names the catalog wants to expose
        self.metadata = metadata or {}
        self.up: dict[str, BaseUserParameter] = user_parameters or {}

    def __getattr__(self, item):
        return self[item]

    def __getitem__(self, item):
        if item in self.aliases:
            item = self.aliases[item]
        return self.entries[item]

    def __iter__(self):
        return iter(self.aliases)

    def __len__(self):
        return len(self.aliases)

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(object.__dir__(self), self.aliases))

    def __add__(self, other: Catalog | DataDescription):
        if not isinstance(other, (Catalog, DataDescription)):
            raise TypeError
        if isinstance(other, DataDescription):
            other = Catalog([other])
        return Catalog(
            entries=chain(self.entries.values(), other.entries.values()),
            aliases=merge_dicts(self.aliases, other.aliases),
            user_parameters=merge_dicts(self.up, other.up),
            metadata=merge_dicts(self.metadata, other.metadata),
        )

    def __iadd__(self, other: Catalog | DataDescription):
        if not isinstance(other, (Catalog, DataDescription)):
            raise TypeError

    def __contains__(self, item: str | DataDescription):
        if isinstance(item, DataDescription):
            item = item.token
        return item in self.entries

    def __setitem__(self, name: str, entry: DataDescription):
        """Add the entry to this catalog with the given alias name

        If the entry is already in the catalog, this effectively just adds an alias. Any existing alias of
        the same name will be clobbered.
        """
        self.entries[entry.token] = entry
        self.aliases[name] = entry.token

    @property
    def data(self):
        return set(e.data for e in self.entries)

    def __call__(self, **kwargs):
        """Makes copy of the catalog with new values for global user parameters"""
        up = self.user_parameters.copy()
        for k, v in kwargs.copy().items():
            if k in self.user_parameters:
                up[k].set_default(v)
                kwargs.pop(k)
        return Catalog(self.entries.values(), self.aliases, user_parameters=up, metadata=self.metadata)

    def rename(self, old, new, clobber=True):
        if not clobber and new in self.aliases:
            raise ValueError
        self.aliases[new] = self.aliases.pop(old)

    def name(self, tok, name, clobber=True):
        if not clobber and name in self.aliases:
            raise ValueError
        if not isinstance(tok, str):
            tok = tok.token
        if tok not in self.entries:
            raise KeyError
        self.aliases[name] = tok
