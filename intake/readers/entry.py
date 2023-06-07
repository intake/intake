"""Description of the ways to load a data set"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from intake import import_name
from intake.readers.datatypes import BaseData
from intake.readers.utils import subclasses


class DataDescription:
    """Defines some data that can be processed

    At minimum, this is a definitions of a datatype instance and kwargs to read it,
    like a classic Intake entry
    """

    def __init__(self, data: BaseData, kwargs_map: dict | None, user_parameters: dict | None = None):
        self.data = data
        self.kwmap: dict[str, dict[str, Any]] = kwargs_map or {}
        self.up = user_parameters or {}

    def select_reader(self, outtype: str | None = None, reader: str | None | type = None) -> type:
        """Pick Reader class

        Rules:
        - if reader is specified, try to find and use that. Can be classname (match lowercase)
          if already subclass of BaseReader, or full path str to class. If not found, error
        - if outtype is given instead (can't have both), find any reader subclass of BaseReader
          which says it implements it for our data type, preferring one with an entry in kwmap
        - if neither is given, pick any reader for our data type, preferring one with an entry
          in kwmap
        """
        from intake.readers.readers import BaseReader

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


class Catalog(Mapping):
    def __getattr__(self, item):
        return self[item]
