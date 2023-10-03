"""Description of the ways to load a data set"""
from __future__ import annotations

from collections.abc import Mapping
from copy import copy
from itertools import chain
from typing import Any, Iterable

import fsspec
import yaml

from intake import import_name
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
        self.metadata = metadata or {}
        self.user_parameters = user_parameters or {}

    def __repr__(self):
        part = f"DataDescription type {self.datatype}\n kwargs {self.kwargs}"
        if self.user_parameters:
            part += f"\n parameters {self.user_parameters}"
        return part

    def to_data(self, user_parameters=None, **kwargs):
        cls = import_name(self.datatype)
        ups = self.user_parameters.copy()
        ups.update(user_parameters or {})
        kw = self.get_kwargs(user_parameters=ups, **kwargs)
        return cls(**kw)

    def __call__(self, **kwargs):
        return self.to_data(**kwargs)

    def get_kwargs(self, user_parameters: dict[str | BaseUserParameter] | None = None, **kwargs) -> dict[str, Any]:
        """Get set of kwargs for given reader, based on prescription, new args and user parameters

        Here, `user_parameters` is intended to come from the containing catalog. To provide values
        for a user parameter, include it by name in kwargs
        """
        kw = self.kwargs.copy()
        kw.update(kwargs)
        up = self.user_parameters.copy()
        up.update(user_parameters or {})
        kw = set_values(up, kw)
        return kw

    def extract_parameter(self, name: str, path: str | None = None, value: Any = None, cls: type = SimpleUserParameter):
        if not ((path is None) ^ (value is None)):
            raise ValueError
        if path is not None:
            kw, up = extract_by_path(path, cls, name, self.kwargs)
        else:
            kw, up = extract_by_value(value, cls, name, self.kwargs)
        self.kwargs = kw
        self.user_parameters[name] = up


class ReaderDescription(Tokenizable):
    def __init__(
        self,
        reader: str,
        kwargs: dict[str, Any] | None = None,
        user_parameters: dict[str | BaseUserParameter] | None = None,
        metadata: dict | None = None,
        output_instance: str | None = None,
    ):
        self.reader = reader
        self.kwargs = kwargs or dict[str, Any]()
        self.output_instance = output_instance
        self.user_parameters: dict[str | BaseUserParameter] = user_parameters or {}
        self.metadata = metadata or {}

    def get_kwargs(self, user_parameters=None, **kwargs) -> dict[str, Any]:
        """Get set of kwargs for given reader, based on prescription, new args and user parameters

        Here, `user_parameters` is intended to come from the containing catalog. To provide values
        for a user parameter, include it by name in kwargs
        """
        kw = self.kwargs.copy()
        user_parameters = user_parameters or {}
        kw.update(kwargs)

        # make data instance
        up = user_parameters or {}  # global/catalog
        if "data" in kw and isinstance(kw["data"], DataDescription):
            extra = kw["data"]
            up.update(kw["data"].user_parameters)
            kw_subset = {k: v for k, v in kwargs.items() if k in user_parameters or k in extra}
            kw["data"] = kw["data"].to_data(user_parameters=user_parameters, **kw_subset)
        else:
            extra = {}
        kw["output_instance"] = self.output_instance

        # now make reader
        up.update(user_parameters)
        kw = set_values(up, kw)
        return kw

    def extract_parameter(self, name: str, path=None, value=None, cls=SimpleUserParameter):
        """Creates new version of the description

        Creates new instance, since the token will in general change
        """
        if not ((path is None) ^ (value is None)):
            raise ValueError
        if path is not None:
            kw, up = extract_by_path(path, cls, name, self.kwargs)
        else:
            kw, up = extract_by_value(value, cls, name, self.kwargs)
        self.kwargs = kw
        self.user_parameters[name] = up

    def to_reader(self, user_parameters=None, **kwargs):
        cls = import_name(self.reader)
        if "data" in kwargs and isinstance(kwargs["data"], DataDescription):
            # if not, is already a BaseData
            ups = kwargs["data"].user_parameters.copy()
        else:
            ups = {}
        ups.update(self.user_parameters)
        ups.update(user_parameters or {})
        kw = self.get_kwargs(user_parameters=ups, **kwargs)
        return cls(**kw)

    def __call__(self, user_parameters=None, **kwargs):
        return self.to_reader(user_parameters=user_parameters, **kwargs)

    @classmethod
    def from_dict(cls, data):
        # note that there should never be any embedded intake classes in kwargs, as they get pulled out
        # when any reader is added to to a catalog
        obj = super().from_dict(data)
        obj.user_parameters = {k: BaseUserParameter.from_dict(v) for k, v in data["user_parameters"].items()}
        return obj

    def __repr__(self):
        extra = f"\n  parameters: {self.user_parameters}" if self.user_parameters else ""
        return f"Entry for reader: {self.reader}\n  kwargs: {self.kwargs}\n" f"  producing: {self.output_instance}" + extra


class Catalog(Tokenizable):
    def __init__(
        self,
        entries: Iterable[ReaderDescription] | Mapping | None = None,
        aliases: dict[str, int] | None = None,
        data: Iterable[DataDescription] | Mapping = None,
        user_parameters: dict[str, BaseUserParameter] | None = None,
        parameter_overrides: dict[str, Any] | None = None,
        metadata: dict | None = None,
    ):
        self.version = 2
        self.data: dict = data or {}
        self.aliases: dict = aliases or {}
        self.metadata: dict = metadata or {}
        self.user_parameters: dict[str, BaseUserParameter] = user_parameters or {}
        self._up_overrides: dict = parameter_overrides or {}
        if isinstance(entries, Mapping) or entries is None:
            self.entries = entries or {}
        else:
            self.entries = {}
            [self.add_entry(e) for e in entries]

    def add_entry(self, entry, name=None):
        """Add entry/reader (and its requirements) in-place, with optional alias"""
        from intake.readers import BaseData, BaseReader
        from intake.readers.utils import find_funcs

        tokens = {}
        if isinstance(entry, (BaseReader, BaseData)):
            # process all readers hidden within the entry as instances
            # Maybe only for readers, not data?
            entry = entry.to_entry()
            entry.kwargs = find_funcs(entry.kwargs, tokens)
        for thing in tokens.values():
            if thing not in self:
                self.add_entry(thing)
        if isinstance(entry, ReaderDescription):
            self.entries[entry.token] = entry
        elif isinstance(entry, DataDescription):
            self.data[entry.token] = entry
        else:
            raise ValueError

        if name:
            self.aliases[name] = entry.token
        return entry.token

    def extract_parameter(
        self,
        item: str,
        name: str,
        path=None,
        value=None,
        cls=SimpleUserParameter,
        store_to: str | None = None,
    ):
        # TODO: if entity is "Catalog", extract over all entities; currently this will
        #  cause a recursion loop
        entity = self.get_entity(item)
        entity.extract_parameter(name, path=path, value=value, cls=cls)
        if store_to is None:
            return
        elif store_to == "data" and isinstance(entity, ReaderDescription):
            entity.kwargs["data"].user_parameters[name] = entity.user_parameters.pop(name)
        else:
            self.move_parameter(item, store_to, name)

    def move_parameter(self, from_entity: str, to_entity: str, parameter_name: str) -> Catalog:
        """Move user-parameter from between entry/data

        `entity` is an alias name or entry/data token
        """
        entity1 = self.get_entity(from_entity)
        entity2 = self.get_entity(to_entity)
        entity2.user_parameters[parameter_name] = entity1.user_parameters.pop(parameter_name)
        return self

    def promote_parameter_name(self, parameter_name: Any, level="cat") -> Catalog:
        """Find and promote given named parameter, assuming they are all identical"""
        up = None
        ups = None
        if level not in ("cat", "data"):
            raise ValueError
        for entity in self.entries.values():
            if parameter_name in entity.user_parameters and up is None:
                ups = entity.user_parameters[parameter_name]
                up = entity.user_parameters[parameter_name]
                entity0 = entity
            elif parameter_name in entity.user_parameters and up == entity.user_parameters[parameter_name]:
                continue
            elif parameter_name in entity.user_parameters:
                ups[parameter_name] = up  # rewind
                raise ValueError
        for entity in self.data.values():
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
            entity0.kwargs["data"].user_parameters[parameter_name] = up
        return self

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            pass
        raise AttributeError(item)

    def to_yaml_file(self, path, **storage_options):
        with fsspec.open(path, mode="wt", **storage_options) as stream:
            yaml.safe_dump(self.to_dict(), stream)

    @staticmethod
    def from_yaml_file(path, **storage_options):
        with fsspec.open(path, **storage_options) as stream:
            cat = Catalog.from_dict(yaml.safe_load(stream))
        cat.user_parameters["CATALOG_DIR"] = path.split("/", 1)[0]
        cat.user_parameters["STORAGE_OPTIONS"] = storage_options
        return cat

    @classmethod
    def from_dict(cls, data):
        """Assemble catalog from dict representation"""
        cat = cls()
        for key, clss in zip(["entries", "data", "user_parameters"], [ReaderDescription, DataDescription, BaseUserParameter]):
            for k, v in data[key].items():
                desc = clss.from_dict(v)
                desc._tok = k
                getattr(cat, key)[k] = desc
        cat.aliases = data["aliases"]
        cat.metadata = data["metadata"]
        return cat

    def get_entity(self, item: str):
        if item == "Catalog":
            return self
        if item in self.aliases:
            item = self.aliases[item]
        if item in self.entries:
            return self.entries[item]
        elif item in self.data:
            return self.data[item]
        else:
            raise KeyError(item)

    def __getitem__(self, item):
        ups = self.user_parameters.copy()
        kw = self._up_overrides.copy()
        if item in self.aliases:
            item = self.aliases[item]
        if item in self.entries:
            item = copy(self.entries[item])
            # TODO: does not pass data's UPs to reader instantiation because data is still a str,
            #  but could grab from self.data
            # ups.update(item.data.user_parameters)
            item = self._rehydrate(item)
            return item(user_parameters=ups, **(kw or {}))
        elif item in self.data:
            item = self.data[item]
            item = self._rehydrate(item)
            return item.to_data(user_parameters=ups, **(kw or {}))
        else:
            raise KeyError(item)

    def _rehydrate(self, val):
        """For any "data" references in the value, replace with"""
        import re

        from intake.readers.entry import DataDescription, ReaderDescription

        if isinstance(val, dict):
            return {k: self._rehydrate(v) for k, v in val.items()}
        elif isinstance(val, str):
            m = re.match(r"{?data[(]([^)]+)[)]}?", val)
            if m:
                return self[m.groups()[0]]
            return val
        elif isinstance(val, bytes):
            return val
        elif isinstance(val, (tuple, set, list)):
            return type(val)(self._rehydrate(v) for v in val)
        elif isinstance(val, (DataDescription, ReaderDescription)):
            val2 = copy(val)
            val2.__dict__ = self._rehydrate(val.__dict__)
            return val2
        return val

    def __delitem__(self, key):
        # remove alias, data or entry with no further actions
        self.aliases.pop(key)
        self.data.pop(key)
        self.entries.pop(key)

    def __delattr__(self, item):
        del self[item]

    def __contains__(self, thing):
        if hasattr(thing, "token"):
            thing = thing.token
        return thing in self.data or thing in self.entries or thing in self.aliases

    def __call__(self, **kwargs):
        """Set override values for any named user parameters

        Returns a new instance of Catalog with overrides set
        """
        up_over = self._up_overrides.copy()
        up_over.update(kwargs)

        new = Catalog(
            entries=self.entries, aliases=self.aliases, data=self.data, user_parameters=self.user_parameters, parameter_overrides=up_over, metadata=self.metadata
        )
        return new

    def __iter__(self):
        return iter(self.aliases)

    def __len__(self) -> int:
        return len(self.aliases)

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(object.__dir__(self), self.aliases))

    def __add__(self, other: Catalog | DataDescription):
        if not isinstance(other, (Catalog, DataDescription)):
            raise TypeError
        if isinstance(other, (DataDescription, ReaderDescription)):
            other = Catalog(entries=[other])
        return Catalog(
            entries=chain(self.entries.values(), other.entries.values()),
            aliases=merge_dicts(self.aliases, other.aliases),
            data=merge_dicts(self.data, other.data),
            user_parameters=merge_dicts(self.user_parameters, other.user_parameters),
            metadata=merge_dicts(self.metadata, other.metadata),
        )

    def __iadd__(self, other: Catalog | ReaderDescription):
        if not isinstance(other, Catalog):
            other = Catalog([other])
        self.entries.update(other.entries)
        self.aliases.update(other.aliases)
        self.user_parameters.update(other.user_parameters)
        self.metadata.update(other.metadata)
        return self

    def __repr__(self):
        txt = f"{type(self).__name__}\n named datasets: {sorted(self.aliases)}"
        if self.user_parameters:
            txt = txt + f"\n  parameters: {sorted(self.user_parameters)}"
        return txt

    def __setitem__(self, name: str, entry):
        """Add the entry to this catalog with the given alias name

        If the entry is already in the catalog, this effectively just adds an alias. Any existing alias of
        the same name will be clobbered.
        """
        self.add_entry(entry, name=name)

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

    # TODO: methods to split a pipeline into sequence of entries and to rejoin them