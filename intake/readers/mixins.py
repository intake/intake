"""Helpers for creating pipelines"""

from __future__ import annotations

from itertools import chain

from intake import import_name
from intake.readers.utils import Completable


class PipelineMixin(Completable):
    """Make it possible to associate transforms with the given class"""

    def __getattr__(self, item):
        super().tab_completion_fixer(item)
        try:
            if item in dir(self.transform):
                return getattr(self.transform, item)
            if "Catalog" in self.output_instance:
                # a better way to mark this condition, perhaps the datatype's structure?
                out = self.read()[item]
            elif item in self._namespaces:
                out = self._namespaces[item]
            # the following can go very wrong - only allow via explicit opt-in?
            else:
                out = self.transform.__getattr__(item)  # arbitrary method call
        except RecursionError as e:
            raise AttributeError(item) from e
        else:
            return out

    def __getitem__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import GetItem

        outtype = self.output_instance
        if "Catalog" in outtype:
            # a better way to mark this condition, perhaps the datatype's structure?
            # TODO: this prevents from doing a transform/convert on a cat, so must use
            #  .transform for that
            return self.read()[item]
        if isinstance(self, Pipeline):
            return self.with_step((GetItem, (item,), {}), out_instance=outtype)

        return Pipeline(
            steps=[(self, (), {}), (GetItem, (item,), {})],
            out_instances=[self.output_instance, outtype],
            metadata=self.metadata,
        )

    def __dir__(self):
        return sorted(chain(object.__dir__(self), dir(self.transform), self._namespaces))

    @property
    def _namespaces(self):
        from intake.readers.namespaces import get_namespaces

        return get_namespaces(self)

    @classmethod
    def output_doc(cls):
        """Doc associated with output type"""
        out = import_name(cls.output_instance)
        return out.__doc__

    def apply(self, func, *args, output_instance=None, **kwargs):
        """Make a pipeline by applying a function to this reader's output"""
        from intake.readers.convert import GenericFunc, Pipeline

        kwargs["func"] = func

        return Pipeline(
            steps=[(self, (), {}), (GenericFunc, args, kwargs)],
            out_instances=[
                self.output_instance,
                output_instance or self.output_instance,
            ],
            metadata=self.metadata,
        )

    @property
    def transform(self):
        from intake.readers.convert import convert_classes

        funcdict = convert_classes(self.output_instance)
        return Functioner(self, funcdict)


class Functioner(Completable):
    """Find and apply transform functions to reader output"""

    def __init__(self, reader, funcdict):
        self.reader = reader
        self.funcdict = funcdict

    def _ipython_key_completions_(self):
        return list(self.funcdict)

    def __getitem__(self, item):
        from intake.readers.convert import Pipeline
        from intake.readers.transform import GetItem

        # TODO: allow pattern match
        if item in self.funcdict:
            func = self.funcdict[item]
            arg = ()
            kw = {}
        else:
            func = GetItem
            arg = (item,)
            kw = {}
        if isinstance(self.reader, Pipeline):
            return self.reader.with_step((func, (), kw), out_instance=item)

        return Pipeline(
            steps=[(self.reader, (), {}), (func, arg, kw)],
            out_instances=[self.reader.output_instance, item],
            metadata=self.reader.metadata,
        )

    def __repr__(self):
        import pprint

        return f"Transformers for {self.reader.output_instance}:\n{pprint.pformat(self.funcdict)}"

    def __call__(self, func, *args, output_instance=None, **kwargs):
        from intake.readers.convert import Pipeline

        if isinstance(self.reader, Pipeline):
            return self.reader.with_step((func, args, kwargs), out_instance=output_instance)
        # TODO: get output_instance from func, if possible

        return Pipeline(
            steps=[(self.reader, (), {}), (func, args, kwargs)],
            out_instances=[self.reader.output_instance, output_instance],
            metadata=self.reader.metadata,
        )

    def methods(self):
        """Methods and attributes associated with the output_instance"""
        try:
            cls = import_name(self.reader.output_instance)
            dnames = (_ for _ in dir(cls) if not _.startswith("_"))
        except (ImportError, AttributeError):
            dnames = []
        return dnames

    def __dir__(self):
        return list(
            sorted(set(chain((f.__name__ for f in self.funcdict.values()), self.methods())))
        )

    def __getattr__(self, item):
        super().tab_completion_fixer(item)
        from intake.readers.convert import Pipeline
        from intake.readers.transform import Method

        out = [(outtype, func) for outtype, func in self.funcdict.items() if func.__name__ == item]
        if not len(out):
            outtype = self.reader.output_instance
            func = Method
            kw = {"method_name": item}
        else:
            outtype, func = out[0]
            kw = {}
        try:
            if isinstance(self.reader, Pipeline):
                out = self.reader.with_step((func, (), kw), out_instance=outtype)
            else:
                out = Pipeline(
                    steps=[(self.reader, (), {}), (func, (), kw)],
                    out_instances=[self.reader.output_instance, outtype],
                    metadata=self.reader.metadata,
                )
        except RecursionError as e:
            raise AttributeError from e
        else:
            return out
