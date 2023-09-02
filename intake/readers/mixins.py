from __future__ import annotations

from itertools import chain

from intake import import_name
from intake.readers import datatypes


class PipelineMixin:
    def __getattr__(self, item):
        if "Catalog" in self.output_instance:
            # a better way to matk this condition, perhaps the datatype's structure?
            return self.read()[item]
        if item in self._namespaces:
            return self._namespaces[item]
        return self.transform.__getattr__(item)

    def __getitem__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import getitem

        outtype = self.output_instance
        if "Catalog" in outtype:
            # a better way to mark this condition, perhaps the datatype's structure?
            # TODO: this prevents from doing a transform/convert on a cat, so must use
            #  .transform for that
            return self.read()[item]
        func = getitem
        if isinstance(self, Pipeline):
            return self.with_step((func, {"item": item}), out_instance=outtype)

        return Pipeline(data=datatypes.ReaderData(reader=self), steps=[(func, {"item": item})], out_instances=[outtype])

    def __dir__(self):
        return list(sorted(chain(object.__dir__(self), dir(self.transform), self._namespaces)))

    @property
    def _namespaces(self):
        from intake.readers.namespaces import get_namespaces

        return get_namespaces(self)

    @classmethod
    def output_doc(cls):
        """Doc associated with output type"""
        out = import_name(cls.output_instance)
        return out.__doc__

    def apply(self, func, output_instance=None, **kwargs):
        """Make a pipeline by applying a function to this reader's output"""
        from intake.readers.convert import Pipeline

        return Pipeline(datatypes.ReaderData(reader=self), [(func, kwargs)], [output_instance or self.output_instance])

    @property
    def transform(self):
        from intake.readers.convert import convert_funcs

        funcdict = convert_funcs(self.output_instance)
        return Functioner(self, funcdict)


class Functioner:
    """Find and apply transform functions to reader output"""

    def __init__(self, reader, funcdict):
        self.reader = reader
        self.funcdict = funcdict

    def _ipython_key_completions_(self):
        return list(self.funcdict)

    def __getitem__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import getitem

        if item in self.funcdict:
            func = self.funcdict[item]
            kw = {}
        else:
            func = getitem
            kw = {"item": item}
        if isinstance(self.reader, Pipeline):
            return self.reader.with_step((func, kw), out_instance=item)

        return Pipeline(data=datatypes.ReaderData(reader=self.reader), steps=[(func, kw)], out_instances=[item])

    def __repr__(self):
        import pprint

        # TODO: replace .*/SameType outputs with out output_instance
        return f"Transformers for {self.reader.output_instance}:\n{pprint.pformat(self.funcdict)}"

    def __dir__(self):
        return list(sorted(f.__name__ for f in self.funcdict.values()))

    def __getattr__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import method

        out = [(outtype, func) for outtype, func in self.funcdict.items() if func.__name__ == item]
        if not len(out):
            outtype = self.reader.output_instance
            func = method
            kw = {"method_name": item}
        else:
            outtype, func = out[0]
            kw = {}
        if isinstance(self.reader, Pipeline):
            return self.reader.with_step((func, kw), out_instance=outtype)

        return Pipeline(data=datatypes.ReaderData(reader=self.reader), steps=[(func, kw)], out_instances=[outtype])
