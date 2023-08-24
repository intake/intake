"""Convert between python representations of data

By convention, functions here do not change the data, just how it is held.
"""
from __future__ import annotations

import re

from intake.readers import readers

_converted = {}


def outtypes():
    """All available types we can convert *to*"""
    return set(_[1] for _ in _converted)


def can_provide(outtype: str):
    """What input types can make the given output type"""
    return set(intype for intype, out in _converted if out == outtype)


# TODO: readers are found by the subclasses of a base and class attributes, but here we have a
#  dict and simple functions. Should have same pattern? Many of these are simple delegation to
#  a method/function, so at least should auto-generate docstring like with functools.wraps .


def register_converter(intype: str, outtype: str, clobber=True):
    """Add a convert function to the list of known conversions"""
    if not clobber and (intype, outtype) in _converted:
        raise ValueError

    def f(func):
        _converted[(intype, outtype)] = func
        return func

    return f


class SameType:
    """Used to indicate that the output of a transform is the same as the input, which is arbitrary"""


@register_converter("duckdb:DuckDBPyRelation", "pandas:DataFrame")
def duck_to_pandas(x, **kw):
    return x.df(**kw)


@register_converter("dask.dataframe:DataFrame", "pandas:DataFrame")
@register_converter("dask.array:Array", "numpy:ndarray")
def daskdf_to_pandas(x, **kw):
    return x.compute(**kw)


@register_converter("pandas:DataFrame", "holoviews.core.layout:Composable")
@register_converter("dask.dataframe:DataFrame", "holoviews.core.layout:Composable")
@register_converter("xarray:DataSet", "holoviews.core.layout:Composable")
def to_hvplot(x, explorer=False, **kw):
    import hvplot

    if explorer:
        # this is actually a hvplot.ui:hvPlotExplorer and only allows tabular data
        return hvplot.explorer(x, **kw)
    return hvplot.hvPlot(x, **kw)()


@register_converter("ray.data:Dataset", "pandas:DataFrame")
def ray_to_pandas(x, **kw):
    return x.to_pandas(**kw)


@register_converter("ray.data:Dataset", "dask.dataframe:DataFrame")
def ray_to_daskdf(x, **kw):
    return x.to_dask(**kw)


@register_converter("tiled.client.node:Node", "intake.readers.entry:Catalog")
def tiled_node_to_cat(x, **kw):
    # eager creation of entries from a node
    from intake.readers.datatypes import TiledDataset, TiledService
    from intake.readers.entry import Catalog
    from intake.readers.readers import TiledClient, TiledNode

    cat = Catalog()
    for k, client in x.items():
        if type(client).__name__ == "Node":
            data = TiledService(url=client.uri)
            reader = TiledNode(data=data, metadata=client.item)
            cat[k] = reader
        else:
            data = TiledDataset(url=client.uri)
            reader = TiledClient(data, output_instance=f"{type(client).__module__}:{type(client).__name__}", metadata=client.item)
            cat[k] = reader
    return cat


def converts_to(data):
    """What things can data convert to"""
    out = set()
    package = type(data).__module__.split(".", 1)[0]
    for intype, outt in _converted:
        if intype.split(".", 1)[0] != package:
            continue
        thing = readers.import_name(intype)
        if isinstance(data, thing):
            out.add(outt)
    return out


def convert_func(data, outtype: str):
    """Get conversion function from given data to outtype

    his works on concrete data, not a datatype or reader instance
    """
    package = type(data).__module__.split(".", 1)[0]
    for intype, out in _converted:
        if out != outtype:
            continue
        if intype.split(".", 1)[0] != package:
            continue
        thing = readers.import_name(intype)
        if isinstance(data, thing):
            return _converted[(intype, out)]
    raise ValueError("Converter not found")


def convert_funcs(in_type: str):
    """Get available conversion functions for input type"""
    out_dict = {}
    for intype, out in _converted:
        if re.match(intype, in_type) or re.match(in_type, intype):
            out_dict[out] = _converted[(intype, out)]
    return out_dict


def convert(data, outtype: str, **kwargs):
    """Convert this data to given type

    This works on concrete data, not a datatype or reader instance
    """
    func = convert_func(data, outtype)
    return func(data, **kwargs)


class Pipeline(readers.BaseReader):
    """Holds a list of transforms/conversions to be enacted in sequence

    A transform on a pipeline makes a new pipeline with that transform added to the sequence
    of operations.
    """

    def __init__(self, data, steps: list[tuple[callable, dict]], out_instances: list[str], entry=None, **kwargs):
        if isinstance(data.reader, Pipeline):
            self.reader = data.reader.reader
            self.steps = data.reader.steps + steps
            out_instances = data.reader.output_instances + out_instances
            data = data.reader.data
        else:
            self.reader = data.reader
            self.steps = steps
            out_instances = out_instances
        # TODO: the output instance may be derivable for SameType or other dynamic use
        super().__init__(data=data, steps=steps, out_instances=out_instances, output_instance=out_instances[-1])
        self.output_instances = []
        prev = self.reader.output_instance
        for inst in out_instances:
            if inst is SameType:
                inst = prev
            prev = inst
            self.output_instances.append(inst)
        steps[-1][1].update(kwargs)
        self.entry = entry

    def __repr__(self):
        start = f"PipelineReader: \nfrom {self.reader}"
        bits = [f"  {i}: {f.__name__}, {kw} => {out}" for i, ((f, kw), out) in enumerate(zip(self.steps, self.output_instances))]
        return "\n".join([start] + bits)

    @property
    def tokens(self):
        return [self.first_n_stages(n).token for n in range(len(self.steps))]

    def output_doc(self):
        from intake import import_name

        out = import_name(self.output_instance)
        return out.__doc__

    def doc(self):
        return self.doc_n(-1)

    def doc_n(self, n):
        self.steps[n][0].__doc__

    def discover(self, **kwargs):
        data = self.reader.discover()
        for i, (func, kw) in enumerate(self.steps):
            if i == len(self.steps) - 1:
                # kwargs passed here override only the last stage
                kw = dict(**kw, **kwargs)
            data = func(data, **kw)
        return data

    def read(self, **kwargs):
        from intake.readers.readers import BaseReader

        data = self.reader.read()
        for i, (func, kw) in enumerate(self.steps):
            kw2 = kw.copy()
            for k, v in kw.items():
                if isinstance(v, BaseReader):
                    kw2[k] = v.read()
                else:
                    kw2[k] = v
            if i == len(self.steps) - 1:
                # kwargs passed here override only the last stage
                kw2 = dict(**kw2, **kwargs)
            data = func(data, **kw2)
        return data

    def apply(self, func, output_instance=None, **kwargs):
        """Add a pipeline stage applying function to the pipeline output so far"""
        return self.with_step((func, kwargs), output_instance or self.output_instance)

    def first_n_stages(self, n: int):
        """Truncate pipeline to the given stage

        If n is equal to the number of steps, this is a simple copy.
        """
        if n < 1 or n > len(self.steps):
            raise ValueError(f"n must be between {1} and {len(self.steps)}")

        return Pipeline(self.data, self.steps[:n], self.output_instances[:n], entry=self.entry, **self.kwargs)

    def with_step(self, step, out_instance):
        if not isinstance(step, tuple):
            # must be a func - check?
            step = (step, {})
        return Pipeline(data=self.data, steps=self.steps + [step], out_instances=self.output_instances + [out_instance])
