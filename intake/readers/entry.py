"""Description of the ways to load a data set"""
from __future__ import annotations

from collections.abc import Mapping
from copy import copy
from itertools import chain
from typing import Any, Iterable

from intake import import_name
from intake.readers.readers import BaseReader
from intake.readers.user_parameters import (
    BaseUserParameter,
    SimpleUserParameter,
    set_values,
)
from intake.readers.utils import (
    Tokenizable,
    extract_by_path,
    extract_by_value,
    merge_dicts,
)


class DataDescription(Tokenizable):
    """Defines some data and a single way to load it, with parameters not yet resolved"""

    def __init__(self, datatype: str, kwargs: dict = None, metadata: dict = None, user_parameters: dict = None):
        self.datatype = datatype
        self.kwargs = kwargs or {}
        self._metadata = metadata or {}
        self._user_parameters = user_parameters or {}

    def __repr__(self):
        part = f"DataDescription {self.datatype} {self.kwargs}"
        if self.user_parameters:
            part += f"\n {self.user_parameters}"
        return part

    def to_data(self, user_parameters=None, **kwargs):
        cls = import_name(self.datatype)
        kw = self.get_kwargs(user_parameters=user_parameters, **kwargs)
        return cls(**kw)

    @property
    def metadata(self):
        return self._metadata

    def __call__(self, **kwargs):
        return self.to_data(**kwargs)

    def get_kwargs(self, user_parameters: dict[str | BaseUserParameter] | None = None, **kwargs) -> dict[str, Any]:
        """Get set of kwargs for given reader, based on prescription, new args and user parameters

        Here, `user_parameters` is intended to come from the containing catalog. To provide values
        for a user parameter, include it by name in kwargs
        """
        kw = self.kwargs.copy()
        kw.update(kwargs)
        up = self._user_parameters.copy()
        up.update(user_parameters or {})
        kw = set_values(up, kw)
        return kw

    def extract_parameter(self, name: str, path: str | None = None, value: Any = None, cls: type = SimpleUserParameter):
        if not ((path is None) ^ (value is None)):
            raise ValueError
        ups = self.user_parameters.copy()
        if path is not None:
            kw, up = extract_by_path(path, cls, name, self.kwargs)
        else:
            kw, up = extract_by_value(value, cls, name, self.kwargs)
        ups[name] = up
        return DataDescription(self.datatype, kw, metadata=self._metadata, user_parameters=ups)

    @property
    def user_parameters(self):
        return self._user_parameters


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
        self._user_parameters = user_parameters or dict[str | BaseUserParameter]()
        self._metadata = metadata or {}

    def get_kwargs(self, user_parameters=None, **kwargs) -> dict[str, Any]:
        """Get set of kwargs for given reader, based on prescription, new args and user parameters

        Here, `user_parameters` is intended to come from the containing catalog. To provide values
        for a user parameter, include it by name in kwargs
        """
        breakpoint()
        kw = self.kwargs.copy()

        # make data instance
        kw_subset = {k: v for k, v in kwargs.items() if k in user_parameters or k in self.data.user_parameters}
        kw["data"] = self.data.to_data(user_parameters=user_parameters, **kw_subset)

        # now make reader
        kw.update(kwargs)
        up = user_parameters or {}  # global/catalog
        up.update(self.data.user_parameters)
        up.update(self._user_parameters)
        kw = set_values(up, kw)
        return kw

    def extract_parameter(self, name: str, path=None, value=None, level="reader", cls=SimpleUserParameter):
        """Creates new version of the description

        Creates new instance, since the token will in general change
        """
        if level not in ("data", "reader"):
            raise ValueError
        if not ((path is None) ^ (value is None)):
            raise ValueError
        if level == "data":
            data = self.data.extract_parameter(name, path, value, cls)
            if value is not None:
                kw, _ = extract_by_value(value, cls, name, self.kwargs)
            else:
                kw = self.kwargs.copy()
            return ReaderDescription(data, self.reader, kw, self.user_parameters)
        else:
            ups = self.user_parameters.copy()
            if path is not None:
                kw, up = extract_by_path(path, cls, name, self.kwargs)
            else:
                kw, up = extract_by_value(value, cls, name, self.kwargs)
            ups[name] = up
            return ReaderDescription(self.data, self.reader, kw, ups)

    @property
    def user_parameters(self):
        return self._user_parameters

    def to_reader(self, user_parameters=None, **kwargs):
        cls = import_name(self.reader)
        kw = self.get_kwargs(user_parameters=user_parameters, **kwargs)
        return cls(**kw)

    def __call__(self, user_parameters=None, **kwargs):
        return self.to_reader(user_parameters=user_parameters, **kwargs)

    @property
    def metadata(self):
        # reader has own metadata?
        return self.data.metadata

    def to_data(self):
        """Make data instance for what this reader produces"""
        # or should this return a DataDescription?
        from intake.readers.datatypes import ReaderData

        return ReaderData(self.to_reader())

    def __repr__(self):
        return f"Entry for {self.data}\nreader: {self.reader}\nkwargs: {self.kwargs}"

    def __add__(self, other: DataDescription | BaseReader):
        """makes a catalog from any two descriptions"""
        if not isinstance(other, (BaseReader, ReaderDescription)):
            raise TypeError
        if isinstance(other, BaseReader):
            other = other.to_entry()
        return Catalog(entries=(self, other))


class Catalog(Mapping):
    def __init__(
        self,
        entries: Iterable[ReaderDescription] | Mapping | None = None,
        aliases: dict[str, int] | None = None,
        data: Iterable[DataDescription] | Mapping = None,
        user_parameters: dict[str, BaseUserParameter] | None = None,  # global to the catalog
        metadata: dict | None = None,
    ):
        self.data = data or {}  # process/tokenise data if an iterable
        self.aliases = aliases or {}  # names the catalog wants to expose
        if isinstance(entries, Mapping) or entries is None:
            self.entries = {}
            for k, v in entries.items():
                if isinstance(v, BaseReader):
                    v = v.to_entry()
                if k != v.token:
                    self.add_entry(v, name=k)
                else:
                    self.add_entry(v)
        elif isinstance(entries, Iterable):
            self.entries: dict[str, ReaderDescription] = {}
            for e in entries:
                self.add_entry(e)
        else:
            raise TypeError
        self.metadata = metadata or {}
        self.user_parameters: dict[str, BaseUserParameter] = user_parameters or {}

    @classmethod
    def from_dict(cls, entries_dict: dict):
        """Create catalog from {alias: reader} dict"""
        # question: should allow this kind of input in __init__?
        cat = cls()
        for k, v in entries_dict.items():
            cat[k] = v
        return cat

    def to_dict(self):
        """Simplify catalog int {alias: reader} form

        This materialises all user_parameters to their current defaults
        """
        return {self[k] for k in self.aliases}

    def add_entry(self, entry, name=None):
        if isinstance(entry, BaseReader):
            entry = entry.to_entry()
        self.entries[entry.token] = entry
        if entry.data.datatype == "intake.readers.datatypes:ReaderData":
            tok = self.add_entry(entry.data.kwargs["reader"])
            entry.data = "data(%s)" % tok
        else:
            self.data[entry.data.token] = entry.data

        if entry.reader == "intake.readers.convert:Pipeline":
            # walk top-level arguments to functions looking for data deps
            for func, kw in entry.kwargs["steps"]:
                for k, v in kw.copy().items():
                    if isinstance(v, BaseReader):
                        tok = self.add_entry(v)
                        kw[k] = "{data(%s)}" % tok

        if name:
            self.aliases[name] = entry.token
        return entry.token

    def promote_parameter_from(self, entity: str, parameter_name: Any, level="cat") -> Catalog:
        """Move catalog from given entry/data *up*"""
        if level not in ("cat", "data"):
            raise ValueError
        if entity in self.aliases:
            entity = self.entries[self.aliases[entity]]
        elif entity in self.entries:
            entity = self.entries[entity]
        elif entity in self.data:
            entity = self.data[entity]
            assert level != "data"
        else:
            raise KeyError
        up = entity.user_parameters.pop(parameter_name)
        if level == "cat":
            self.user_parameters[parameter_name] = up
        else:
            entity.data.user_parameters[parameter_name] = up
        return self

    def promote_parameter_name(self, parameter_name: Any, level="cat") -> Catalog:
        """Find and promote given named parameter, assuming they are all identical"""
        up = None
        ups = None
        if level not in ("cat", "data"):
            raise ValueError
        for entity in self.entries:
            if parameter_name in entity.user_parameters and up is None:
                ups = entity.user_parameters[parameter_name]
                up = entity.user_parameters[parameter_name]
                entity0 = entity
            elif parameter_name in entity.user_parameters and up == entity.user_parameters[parameter_name]:
                continue
            elif parameter_name in entity.user_parameters:
                ups[parameter_name] = up  # rewind
                raise ValueError
        for entity in self.data:
            if parameter_name in entity.user_parameters and up is None:
                assert level == "cat"
                ups = entity.user_parameters[parameter_name]
                up = entity.user_parameters[parameter_name]
            elif parameter_name in entity.user_parameters and up == entity.user_parameters[parameter_name]:
                continue
            elif parameter_name in entity.user_parameters:
                ups[parameter_name] = up  # rewind
                raise ValueError

        if level == "cat":
            self.user_parameters[parameter_name] = up
        else:
            entity0.data.user_parameters[parameter_name] = up
        return self

    def __getattr__(self, item):
        return self[item]

    def __getstate__(self):
        # maybe straight to a JSON/YAML representation here
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __getitem__(self, item):
        if isinstance(item, tuple):
            item, kw = item
        else:
            kw = {}
        if item in self.aliases:
            item = self.aliases[item]
        if item in self.entries:
            item = copy(self.entries[item])
            if isinstance(item.data, str) and item.data.startswith("data(") and item.data.endswith(")"):
                item.data = self.entries[item.data[5:-1]]
            return item(user_parameters=dict(**self.user_parameters, **self.entries), **(kw or {}))
        elif item in self.data:
            return self.data[item].to_data(user_parameters=self.user_parameters, **(kw or {}))
        else:
            raise KeyError(item)

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
            user_parameters=merge_dicts(self.user_parameters, other.up),
            metadata=merge_dicts(self.metadata, other.metadata),
        )

    def __iadd__(self, other: Catalog | ReaderDescription | BaseReader):
        if not isinstance(other, (Catalog, ReaderDescription, BaseReader)):
            raise TypeError
        if not isinstance(other, Catalog):
            other = Catalog([other])
        self.entries.update(other.entries)
        self.aliases.update(other.aliases)
        self.user_parameters.update(other.up)
        self.metadata.update(other.metadata)
        return self

    def __contains__(self, item: str | DataDescription):
        if isinstance(item, DataDescription):
            item = item.token
        return item in self.entries or item in self.aliases

    def __repr__(self):
        txt = f"{type(self).__name__}\n named datasets: {sorted(self.aliases)}"
        if self.user_parameters:
            txt = txt + f"\n user parameters: {sorted(self.user_parameters)}"
        return txt

    def __setitem__(self, name: str, entry: DataDescription):
        """Add the entry to this catalog with the given alias name

        If the entry is already in the catalog, this effectively just adds an alias. Any existing alias of
        the same name will be clobbered.
        """
        self.add_entry(entry, name=name)

    def data(self):
        return {e.data for e in self.entries.values()}

    def __call__(self, **kwargs):
        """Makes copy of the catalog with new values for global user parameters"""
        up = self.user_parameters.copy()
        for k, v in kwargs.copy().items():
            if k in self.user_parameters:
                up[k] = up.with_default(v)
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
