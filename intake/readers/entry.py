"""Description of the ways to load a data set"""
from __future__ import annotations

from collections.abc import Mapping
from itertools import chain
from typing import Any, Iterable, NamedTuple

from intake import import_name
from intake.readers.datatypes import BaseData
from intake.readers.readers import BaseReader
from intake.readers.user_parameters import BaseUserParameter
from intake.readers.utils import subclasses


class Spec(NamedTuple):
    reader: str
    kwargs: dict[str, Any]


class DataDescription:
    """Defines some data and ways in which it can be handled

    At minimum, this is a definitions of a datatype instance and kwargs to read it,
    like a classic Intake entry.

    More generally, multiple readers and pipelines originating in the base data can be defined,
    or even multiple argument sets for the same readers.
    """

    def __init__(self, data: BaseData, kwargs_map: list[Spec], user_parameters: dict[str | BaseUserParameter] | None = None):
        self.data = data
        self.kwmap: dict[str, dict[str, Any]] = kwargs_map or dict[str, dict[str, Any]]()
        self.up = user_parameters or dict[str | BaseUserParameter]()

    def select_reader(self, outtype: str | None = None, reader: str | None | type = None) -> type:
        """Pick Reader class

        Rules:
        - if reader is specified, try to find and use that. Can be classname (match lowercase)
          if already a subclass of BaseReader, or full path str to class. If not found, error
        - if outtype is given instead (can't have both), find any reader subclass of BaseReader
          which says it implements it for our data type, preferring one with an entry in kwmap
        - if neither is given, pick any reader for our data type, preferring one with an entry
          in kwmap
        """
        if reader and outtype:
            raise ValueError
        if reader:
            if isinstance(reader, type):
                reader_cls = reader
            elif "." in reader or ":" in reader:
                reader_cls = import_name(reader)
            else:
                reader_classes = [cls for cls in subclasses(BaseReader) if cls.__name__.lower() == reader.lower()]
                if reader_classes:
                    reader_cls = reader_classes[0]
                else:
                    raise ValueError
            if type(self.data) not in reader_cls.implements:
                raise ValueError
        elif outtype:
            reader_classes = [cls for cls in subclasses(BaseReader) if type(self.data) in cls.implements and outtype in cls.output_instance]
            if len(reader_classes) > 1:
                reader_classes = [cls for cls in reader_classes if cls.__name__.lower() in self.kwmap] or reader_classes
            elif len(reader_classes) == 0:
                raise ValueError
            reader_cls = reader_classes[0]
        elif self.kwmap:
            reader_classes = [cls for cls in subclasses(BaseReader) if cls.__name__.lower() in self.kwmap]
            if len(reader_classes) == 0:
                raise ValueError
            reader_cls = reader_classes[0]
        else:
            # == self.possible_readers ?
            reader_classes = [cls for cls in subclasses(BaseReader) if type(self.data) in cls.implements]
            if len(reader_classes) > 1:
                reader_classes = [cls for cls in reader_classes if cls.__name__.lower() in self.kwmap] or reader_classes
            elif len(reader_classes) == 0:
                raise ValueError
            reader_cls = reader_classes[0]
        return reader_cls

    def get_kwargs(self, reader_cls, **kwargs) -> dict:
        """Get set of kwargs for given reader, based on prescription, new args and user parameters"""
        kw = self.kwmap.get(reader_cls.__name__.lower(), {}).copy()
        kw["data"] = self.data
        kw.update(kwargs)
        # process user_parameters and template
        return kw

    def get_reader(self, outtype=None, reader=None, **kwargs):
        cls = self.select_reader(outtype=outtype, reader=reader)
        kw = self.get_kwargs(cls, **kwargs)
        return cls(entry=self, **kw)

    @property
    def possible_readers(self):
        from intake.readers import readers

        return readers.recommend(self.data)

    @property
    def defined_readers(self):
        return set(self.kwmap)

    @property
    def metadata(self):
        return self.data.metadata

    def add_reader(self, reader):
        if not reader.data == self:
            # only require types to match?
            raise ValueError("Reader is for a different data definition")
        # TODO: key here is only name; need token and ability to give name
        key = type(reader).__name__.lower()
        kw = reader._kw.copy()
        self.kwmap[key] = kw  # check clobber

    def __repr__(self):
        if self.kwmap:
            part = f"with defined readers {list(self.kwmap)}"
        else:
            part = "with no defined readers"
        return f"Entry for data {self.data}\n" + part

    def __add__(self, other: DataDescription | BaseReader):
        # if other has the same data instance, add its kwargs to produce a DataDescription
        # if not, we make a Catalog
        if not isinstance(other, (DataDescription, BaseReader)):
            raise TypeError

    def __iadd__(self, other: DataDescription | BaseReader):
        # merge in kwargs of the other. If not the same data instance, error
        if not isinstance(other, (DataDescription, BaseReader)):
            raise TypeError
        if other.data != self.data:
            raise ValueError


def merge_entries(*entries: tuple[DataDescription, ...]) -> tuple(DataDescription):
    """Make an entry with kwargs combined from several entries

    This is meant for one data instance, but will work for many, producing a list.
    That output list is a lot like a catalog without explicit names, so this function
    is likely called in the process of making a catalog.
    """
    out = {}
    for entry in entries:
        if entry.data not in out:
            out[entry.data] = entry
        else:
            out[entry.data].kwmap.update(entry.kwmap)
    return list(out.values())


class Catalog(Mapping):
    def __init__(self, entries: dict[int:DataDescription], aliases: dict[str, int] | None = None, user_parameters: dict | None = None, metadata: dict | None = None):
        self.entries = entries  # map raw int tokens to DataDescriptions by their hash
        # TODO: extract out all tokens of data instances and pipeline stages
        # TODO: replace token references with instance counterpart now or on access?
        self.aliases = aliases or {}  # names the catalog wants to expose
        self.metadata = metadata or {}
        self.up = user_parameters or {}

    def __getattr__(self, item):
        return self[item]

    def __getitem__(self, item):
        if item in self.aliases:
            item = self.aliases[item]
        return self.entries[item]

    def __iter__(self):
        return list(self.aliases)

    def __len__(self):
        return len(self.aliases)

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(object.__dir__(self), self.aliases))

    def __add__(self, other: Catalog | DataDescription):
        if not isinstance(other, (Catalog, DataDescription)):
            raise TypeError

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
        pass

    def rename(self, old, new, clobber=True):
        if clobber and new in self.aliases:
            raise ValueError
        self.aliases[new] = self.aliases.pop(old)

    def name(self, tok, name, clobber=True):
        if not isinstance(tok, int):
            tok = hash(tok)
        if clobber and name in self.aliases:
            raise ValueError
        if tok not in self.entries:
            raise ValueError
        self.aliases[name] = tok
