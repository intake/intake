"""Description of the ways to load a data set

These are the definitions as they would appear in a Catalog: they may have (unevaluated)
user parameters and references to one-another, as well as other templated values.

The rule is: when placing an entry in a catalog, it is converted to its constituent data and
reader descriptions. When accessing an entry, it is re-instantiated as a reader. The entries
within a catalog can be amended in-place (such as extracting user parameters or templating)
and persisted as catalog files.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import copy
from itertools import chain
import re
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
    check_imports,
    extract_by_path,
    extract_by_value,
    merge_dicts,
)


class DataDescription(Tokenizable):
    """Defines some data: class and arguments. This may be laoded in a number of ways

    A DataDescription normally resides in a Catalog, and can contain templated arguments.
    When there are user_parameters, these will also be applied to any reader that
    depends on this data.
    """

    def __init__(
        self,
        datatype: str,
        kwargs: dict = None,
        metadata: dict = None,
        user_parameters: dict = None,
    ):
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
        return cls(**kw, metadata=self.metadata)

    def __call__(self, **kwargs):
        return self.to_data(**kwargs)

    def get_kwargs(
        self, user_parameters: dict[str | BaseUserParameter] | None = None, **kwargs
    ) -> dict[str, Any]:
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

    def extract_parameter(
        self,
        name: str,
        path: str | None = None,
        value: Any = None,
        cls: type = SimpleUserParameter,
        **kw,
    ):
        if not ((path is None) ^ (value is None)):
            raise ValueError
        if path is not None:
            kw, up = extract_by_path(path, cls, name, self.kwargs, **kw)
        else:
            kw, up = extract_by_value(value, cls, name, self.kwargs, **kw)
        self.kwargs = kw
        self.user_parameters[name] = up


class ReaderDescription(Tokenizable):
    """
    A serialisable description of a reader or pipeline

    This class is typically stored inside Catalogs, and can contain templated arguments
    which get evaluated at the time that it is accessed from a Catalog.
    """

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

    def check_imports(self):
        """Are the packages listed in the "imports" key of the metadata available?"""
        cls = import_name(self.reader)
        class_import = cls.check_imports()
        meta_imports = check_imports(*self.metadata.get("imports", set()))
        return class_import & meta_imports

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

    def extract_parameter(self, name: str, path=None, value=None, cls=SimpleUserParameter, **kw):
        """Creates new version of the description

        Creates new instance, since the token will in general change
        """
        if not ((path is None) ^ (value is None)):
            raise ValueError
        if path is not None:
            kw, up = extract_by_path(path, cls, name, self.kwargs, **kw)
        else:
            kw, up = extract_by_value(value, cls, name, self.kwargs, **kw)
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
        return cls(metadata=self.metadata, **kw)

    def to_cat(self, name=None):
        """Create a Catalog containing only this entry"""
        cat = Catalog()
        cat.add_entry(self, name)
        return cat

    def __call__(self, user_parameters=None, **kwargs):
        return self.to_reader(user_parameters=user_parameters, **kwargs)

    @classmethod
    def from_dict(cls, data):
        # note that there should never be any embedded intake classes in kwargs, as they get pulled out
        # when any reader is added to to a catalog
        obj = super().from_dict(data)
        obj.user_parameters = {
            k: BaseUserParameter.from_dict(v) for k, v in data["user_parameters"].items()
        }
        return obj

    def __repr__(self):
        extra = f"\n  parameters: {self.user_parameters}" if self.user_parameters else ""
        return (
            f"Entry for reader: {self.reader}\n  kwargs: {self.kwargs}\n"
            f"  producing: {self.output_instance}" + extra
        )


class Catalog(Tokenizable):
    """A collection of data and reader descriptions."""

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

    def add_entry(self, entry, name=None, clobber=True):
        """Add entry/reader (and its requirements) in-place, with optional alias"""
        from intake.readers import BaseData, BaseReader
        from intake.readers.utils import find_funcs, replace_values

        if isinstance(entry, (BaseReader, BaseData)):
            entry = entry.to_entry()
        if entry in self:
            if name and name != entry.token:
                self.aliases[name] = entry.token
            return entry.token
        tokens = {}
        entry.kwargs = find_funcs(entry.kwargs, tokens)
        if name:
            entry._tok = name
        for tok, thing in reversed(tokens.items()):
            thing = thing.to_entry()
            if thing == entry:
                continue
            tok = self.add_entry(thing)
            if tok not in self:
                # did not add new one, because it's already there
                old_tok = next(iter(self._find_iter(thing))).token
                entry.kwargs = replace_values(
                    entry.kwargs, "{data(%s)}" % tok, "{data(%s)}" % old_tok
                )
        if entry in self and clobber is False:
            raise ValueError("Name {} exists in catalog, and clobber is False", entry.token)
        if isinstance(entry, ReaderDescription):
            self.entries[entry.token] = entry
        elif isinstance(entry, DataDescription):
            self.data[entry.token] = entry
        else:
            raise ValueError

        if name and name != entry.token:
            self.aliases[name] = entry.token
        return entry.token

    def _ipython_key_completions_(self):
        return sorted(set(chain(self.aliases, self.data, self.entries)))

    def delete(self, name, recursive=False):
        """Remove named entity (data/entry) from catalog

        We do not check whether any other entity in the catalog refers *to*
        what is being deleted, so you can break other entries this way.

        Parameters
        ----------
        recursive: bool
            Also removed data/entries references by the given one, and those
            they refer to in turn.
        """
        item = self.get_entity(name)
        del self[item]
        if recursive:
            raise NotImplementedError

    def extract_parameter(
        self,
        item: str,
        name: str,
        path: str | None = None,
        value: Any = None,
        cls=SimpleUserParameter,
        store_to: str | None = None,
        **kw,
    ):
        """
        Descend into data & reader descriptions to create a user_parameter

        There are two ways to fund and replace values by a template:

        - if ``path`` is given, the kwargs will be walked to this location
          e.g., "field.0.special_value" -> kwargs["field"][0]["special_value"]
        - if ``value`` is given, all kwargs will be recursively walked, looking
          for values that equal that given.

        Matched values will be replaced by a template string like ``"{name}"``,
        and a user_parameter of class ``cls`` will be placed in the location
        given by ``store_to`` (could be "data", "catalog").
        """
        # TODO: if entity is "Catalog", extract over all entities; currently this will
        #  cause a recursion loop
        entity = self.get_entity(item)
        entity.extract_parameter(name, path=path, value=value, cls=cls, **kw)
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

    def promote_parameter_name(self, parameter_name: str, level: str = "cat") -> Catalog:
        """Find and promote given named parameter, assuming they are all identical

        parameter_name:
            the key string referring to the parameter
        level: cat | data
            If the parameter is found in a reader, it can be promoted to the data it
            depends on. Parameters in a data description can only be promoted to a
            catalog global.
        """
        up = None
        ups = None
        if level not in ("cat", "data"):
            raise ValueError
        for entity in self.entries.values():
            if parameter_name in entity.user_parameters and up is None:
                ups = entity.user_parameters[parameter_name]
                up = entity.user_parameters[parameter_name]
                entity0 = entity
            elif (
                parameter_name in entity.user_parameters
                and up == entity.user_parameters[parameter_name]
            ):
                continue
            elif parameter_name in entity.user_parameters:
                ups[parameter_name] = up  # rewind
                raise ValueError
        for entity in self.data.values():
            if parameter_name in entity.user_parameters and up is None:
                assert level == "cat"
                ups = entity.user_parameters[parameter_name]
                up = entity.user_parameters[parameter_name]
            elif (
                parameter_name in entity.user_parameters
                and up == entity.user_parameters[parameter_name]
            ):
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
        super().tab_completion_fixer(item)
        try:
            return self[item]
        except KeyError:
            pass
        except RecursionError as e:
            raise AttributeError from e
        raise AttributeError(item)

    def to_yaml_file(self, path: str, **storage_options):
        """Persist the state of this catalog as a YAML file

        storage_options:
            kwargs to pass to fsspec for opening the file to write
        """
        # TODO: remove ['CATALOG_DIR', 'CATALOG_PATH', 'STORAGE_OPTIONS'] UPs?
        with fsspec.open(path, mode="wt", **storage_options) as stream:
            yaml.safe_dump(self.to_dict(), stream)

    @staticmethod
    def from_yaml_file(path: str, **kwargs):
        """Load YAML representation into a new Catalog instance

        storage_options:
            kwargs to pass to fsspec for opening the file to read; can pass as storage_options=
            or will pick up any unused kwargs for simplicity
        """
        storage_options = kwargs.pop("storage_options", kwargs)
        of = fsspec.open(path, **storage_options)
        with of as stream:
            cat = Catalog.from_dict(yaml.safe_load(stream))
        cat.user_parameters["CATALOG_PATH"] = path
        cat.user_parameters["CATALOG_DIR"] = of.fs.unstrip_protocol(of.fs._parent(path))
        cat.user_parameters["STORAGE_OPTIONS"] = storage_options
        return cat

    @classmethod
    def from_entries(cls, data: dict, metadata=None):
        """Assemble catalog from a dict of entries"""
        cat = cls(metadata=metadata)
        for k, v in data.items():
            cat[k] = v
        return cat

    @classmethod
    def from_dict(cls, data):
        """Assemble catalog from dict representation"""
        if data.get("version") != 2:
            raise ValueError("Not a V2 catalog")
        cat = cls()
        for key, clss in zip(
            ["entries", "data", "user_parameters"],
            [ReaderDescription, DataDescription, BaseUserParameter],
        ):
            for k, v in data[key].items():
                if isinstance(v, dict):
                    desc = clss.from_dict(v)
                    desc._tok = k
                else:
                    desc = v
                getattr(cat, key)[k] = desc
        cat.aliases = data["aliases"]
        cat.metadata = data["metadata"]
        return cat

    def get_entity(self, item: str):
        """Get the objects by reference

        Use this method if you want to change the catalog in-place

        item can be an entry in .aliases, in which case the original wil be returned,
        or a key in .entries, .user_parameters or .data.
        The entity in question is returned without processing.
        """
        if item.lower() in ["cat", "catalog"]:
            return self
        # TODO: this can be simplified with `get(..) or`
        if item in self.aliases:
            item = self.aliases[item]
        if item in self.entries:
            return self.entries[item]
        elif item in self.data:
            return self.data[item]
        elif item in self.user_parameters:
            return self.user_parameters[item]
        else:
            raise KeyError(item)

    def get_aliases(self, entity: str):
        """Return those alias names that point to the given opaque key"""
        return {k for k, v in self.aliases.items() if v == entity}

    def search(self, expr) -> Catalog:
        """Make new catalog with a subset of this catalog

        The new catalog will have those entries which pass the filter `expr`, which
        is an instance of `intake.readers.search.BaseSearch` (i.e., has a method
        like `filter(entry) -> bool`).

        In the special case that expr is just a string, the `Text` search expression
        will be used.
        """
        from intake.readers.search import Text

        if isinstance(expr, str):
            expr = Text(expr)
        cat = Catalog()
        for e, v in self.entries.items():
            if expr.filter(v):
                cat.add_entry(v)
                aliases = self.get_aliases(e)
                cat.aliases.update({a: e for a in aliases})
        return cat

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
        """Recreate reader instances when accessed from this catalog, filling in refs and templates"""
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
        if key in self.aliases:
            self.data.pop(self.aliases[key], None)
            self.entries.pop(self.aliases[key], None)
        for k, v in self.aliases.copy().items():
            # remove alias pointing TO key
            if v == key:
                self.aliases.pop(k)
        self.aliases.pop(key, None)
        self.data.pop(key, None)
        self.entries.pop(key, None)

    def __delattr__(self, item):
        del self[item]

    def _find_iter(self, thing):
        if isinstance(thing, DataDescription):
            return (_ for _ in self.data.values() if thing.to_dict() == _.to_dict())
        elif isinstance(thing, ReaderDescription):
            return (_ for _ in self.entries.values() if thing.to_dict() == _.to_dict())
        else:
            return ()

    def __contains__(self, thing):
        from intake.readers import BaseReader, BaseData

        if isinstance(thing, (BaseData, BaseReader)):
            thing = thing.to_entry()
        if hasattr(thing, "token"):
            thing0 = thing.token
        else:
            thing0 = thing
        easy = thing0 in self.data or thing0 in self.entries or thing0 in self.aliases
        return easy or any(self._find_iter(thing))

    def __call__(self, **kwargs):
        """Set override values for any named user parameters

        Returns a new instance of Catalog with overrides set
        """
        up_over = self._up_overrides.copy()
        up_over.update(kwargs)

        new = Catalog(
            entries=self.entries,
            aliases=self.aliases,
            data=self.data,
            user_parameters=self.user_parameters,
            parameter_overrides=up_over,
            metadata=self.metadata,
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

    def rename(self, old: str, new: str, clobber=True):
        """Change the alias of a dataset"""
        if not clobber and new in self.aliases:
            raise ValueError
        self.aliases[new] = self.aliases.pop(old)

    @property
    def name(self):
        if not re.match("^[0-9a-f]{16}$", self.token):
            return self.token
        else:
            return self.metadata.get("name", "unnamed")

    def give_name(self, tok: str, name: str, clobber=True):
        """Give an alias to a dataset

        tok:
            a key in the .entries dict
        """
        if not clobber and name in self.aliases:
            raise ValueError
        if not isinstance(tok, str):
            tok = tok.token
        if tok not in self.entries:
            raise KeyError
        self.aliases[name] = tok

    # TODO: methods to split a pipeline into sequence of entries and to rejoin them
