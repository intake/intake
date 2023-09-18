"""Convert between python representations of data

By convention, functions here do not change the data, just how it is held.
"""
from __future__ import annotations

import re
from functools import cache
from itertools import chain

from intake import import_name
from intake.readers import readers
from intake.readers.utils import Tokenizable, all_to_one, subclasses


class ImportsProperty:
    """Builds the .imports attribute from classes in the .instances class attribute"""

    def __get__(self, obj, cls):
        # this asserts that ALL in and out types should be importable
        cls.imports = set(_.split(":", 1)[0].split(".", 1)[0] for _ in chain(cls.instances, cls.instances.values()) if _ is not SameType)
        return cls.imports


class BaseConverter(Tokenizable):
    """Converts from one object type to another

    Most often, subclasses call a single function on the data, but arbitrary complex transforms
    are possible. This is designed to be one step in a Pipeline.

    .run() will be called on the output object from the previous stage, subclasses will wither
    override that, or just provide a func=.
    """

    instances: dict[str, str] = {}
    func: str = ""
    imports = ImportsProperty()

    @classmethod
    def doc(cls):
        start = cls.run.__doc__ or ""
        func = import_name(cls.func).__doc__ or "" if cls.func else ""
        return "\n\n".join([cls.__doc__, start, func])

    def run(self, x, *args, **kwargs):
        func = import_name(self.func)
        return func(x, *args, **kwargs)


class SameType:
    """Used to indicate that the output of a transform is the same as the input, which is arbitrary"""


class DuckToPands(BaseConverter):
    instances = {"duckdb:DuckDBPyRelation": "pandas:DataFrame"}
    func = "duckdb:DuckDBPyConnection.df"


class DaskDFToPandas(BaseConverter):
    instances = {"dask.dataframe:DataFrame": "pandas:DataFrame", "dask.array:Array": "numpy:ndarray"}
    func = "dask:compute"


class PandasToGeopandas(BaseConverter):
    instances = {"pandas:DataFrame": "geopandas:GeoDataFrame"}
    func = "geopandas:GeoDataFrame"


class ToHvPlot(BaseConverter):
    instances = all_to_one({"pandas:DataFrame", "dask.dataframe:DataFrame", "xarray:DataSet", "xarray:DataArray"}, "holoviews.core.layout:Composable")
    func = "hvplot:hvPlot"

    def run(self, x, explorer: bool = False, **kw):
        """For tabular data only, pass explorer=True to get an interactive GUI"""
        import hvplot

        if explorer:
            # this is actually a hvplot.ui:hvPlotExplorer and only allows tabular data
            return hvplot.explorer(x, **kw)
        return hvplot.hvPlot(x, **kw)()


class RayToPandas(BaseConverter):
    instances = {"ray.data:Dataset": "pandas:DataFrame"}
    func = "ray.data:Dataset.to_pandas"


class RayToDask(BaseConverter):
    instances = {"ray.data:Dataset": "dask.dataframe:DataFrame"}
    func = "ray.data:Dataset.to_dask"


class TiledNodeToCatalog(BaseConverter):
    instances = {"tiled.client.node:Node": "intake.readers.entry:Catalog"}

    def run(self, x, **kw):
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


class TiledSearch(BaseConverter):
    """See https://blueskyproject.io/tiled/tutorials/search.html"""

    instances = {"tiled.client.node:Node": "tiled.client.node:Node"}

    def run(self, x, **kw):
        return x.search(**kw)


def converts_to(data):
    """What things can data convert to"""
    out = set()
    package = type(data).__module__.split(".", 1)[0]
    for cls in subclasses(BaseConverter):
        for intype, outtype in cls.instances.items():
            if intype.split(".", 1)[0] != package:
                continue
            thing = readers.import_name(intype)
            if isinstance(data, thing):
                out.add(outtype)
    return out


def convert_class(data, out_type: str):
    """Get conversion class from given data to out_type

    This works on concrete data, not a datatype or reader instance. It returns the
    first match. out_type will match on regex, e.g., "pandas" would match "pandas:DataFrame"
    """
    package = type(data).__module__.split(".", 1)[0]
    for cls in subclasses(BaseConverter):
        for intype, outtype in cls.instances.items():
            if not re.findall(out_type, outtype):
                continue
            if intype.split(".", 1)[0] != package:
                continue
            thing = readers.import_name(intype)
            if isinstance(data, thing):
                return cls
    raise ValueError("Converter not found")


def convert_classes(in_type: str):
    """Get available conversion classes for input type"""
    out_dict = {}
    package = in_type.split(":", 1)[0].split(".", 1)[0]
    for cls in subclasses(BaseConverter):
        for intype, outtype in cls.instances.items():
            if intype.split(".", 1)[0] != package:
                continue
            if re.findall(intype, in_type) or re.findall(in_type, intype):
                out_dict[outtype] = cls
    return out_dict


class Pipeline(readers.BaseReader):
    """Holds a list of transforms/conversions to be enacted in sequence

    A transform on a pipeline makes a new pipeline with that transform added to the sequence
    of operations.
    """

    def __init__(self, data, steps: list[tuple[callable, tuple, dict]], out_instances: list[str], output_instance=None, **kwargs):
        from intake.readers.readers import BaseReader

        if isinstance(data, BaseReader):
            self.reader = data
            self.steps = steps
            out_instances = out_instances
        elif isinstance(data.reader, Pipeline):
            self.reader = data.reader.reader
            self.steps = data.reader.steps + steps
            out_instances = data.reader.output_instances + out_instances
            data = data.reader.data
        else:
            self.reader = data.reader
            self.steps = steps
            out_instances = out_instances
        super().__init__(data=data, steps=steps, out_instances=out_instances, output_instance=out_instances[-1])
        self.output_instances = []
        prev = self.reader.output_instance
        for inst in out_instances:
            if inst is SameType:
                inst = prev
            prev = inst
            self.output_instances.append(inst)
        steps[-1][2].update(kwargs)

    def __repr__(self):
        start = f"PipelineReader: \nfrom {self.reader}"
        bits = [f"  {i}: {f.__name__}, {args} {kw} => {out}" for i, ((f, args, kw), out) in enumerate(zip(self.steps, self.output_instances))]
        return "\n".join([start] + bits)

    @property
    def tokens(self):
        return [(self.token, n) for n in range(len(self.steps))]

    def output_doc(self):
        from intake import import_name

        out = import_name(self.output_instance)
        return out.__doc__

    def doc(self):
        return self.doc_n(-1)

    def doc_n(self, n):
        self.steps[n][0].__doc__

    def discover(self, **_):
        data = self.reader.discover()
        for i in range(len(self.steps)):
            data = self._read_stage_n(data, i)
        return data

    def _read_stage_n(self, data, stage, **kwargs):
        from intake.readers.readers import BaseReader

        func, arg, kw = self.steps[stage]

        kw2 = kw.copy()
        kw2.update(kwargs)
        for k, v in kw.items():
            if isinstance(v, BaseReader):
                kw2[k] = v.read()
            else:
                kw2[k] = v
        if isinstance(func, type) and issubclass(func, BaseConverter):
            return func().run(data, *arg, **kw2)
        else:
            return func(data, *arg, **kw2)

    def _read(self, out_instance=None, out_instances=None, steps=None, **kwargs):
        data = self.reader.read()
        for i in range(len(self.steps)):
            kw = kwargs if i == len(self.steps) - 1 else {}
            data = self._read_stage_n(data, i, **kw)
        return data

    def apply(self, func, *arg, output_instance=None, **kwargs):
        """Add a pipeline stage applying function to the pipeline output so far"""
        return self.with_step((func, arg, kwargs), output_instance or self.output_instance)

    def first_n_stages(self, n: int):
        """Truncate pipeline to the given stage

        If n is equal to the number of steps, this is a simple copy.
        """
        # TODO: allow n=0 to get the basic reader?
        if n < 1 or n > len(self.steps):
            raise ValueError(f"n must be between {1} and {len(self.steps)}")

        pipe = Pipeline(self.data, self.steps[:n], self.output_instances[:n], **self.kwargs)
        if n < len(self.steps):
            pipe.token = (self.token, n)
        return pipe

    def with_step(self, step, out_instance):
        if not isinstance(step, tuple):
            # must be a func - check?
            step = (step, (), {})
        return Pipeline(data=self.data, steps=self.steps + [step], out_instances=self.output_instances + [out_instance])


@cache
def conversions_graph():
    import networkx

    from intake.readers.utils import subclasses

    graph = networkx.DiGraph()

    # transformers
    nodes = set(cls.output_instance for cls in subclasses(readers.BaseReader) if cls.output_instance)
    graph.add_nodes_from(nodes)

    for cls in subclasses(readers.BaseReader):
        if cls.output_instance:
            for impl in cls.implements:
                graph.add_node(cls.output_instance)
                graph.add_edge(impl.qname(), cls.output_instance, label=cls.qname())
    for cls in subclasses(BaseConverter):
        for inttype, outtype in cls.instances.items():
            if inttype != ".*" and inttype != outtype:
                graph.add_nodes_from((inttype, outtype))
                graph.add_edge(inttype, outtype, label=cls.qname())

    return graph


def plot_conversion_graph(filename) -> None:
    # TODO: return a PNG datatype or something else?
    import networkx as nx

    g = conversions_graph()
    a = nx.nx_agraph.to_agraph(g)  # requires pygraphviz
    a.draw(filename, prog="fdp")


def path(start: str, end: str, cutoff: int = 5) -> list:
    import networkx as nx

    alltypes = list(conversions_graph())
    matchtypes = [_ for _ in alltypes if re.findall(start, _.lower)]
    if not matchtypes:
        raise ValueError("type found no match: %s", start)
    start = matchtypes[0]
    matchtypes = [_ for _ in alltypes if re.findall(end, _.lower)]
    if not matchtypes:
        raise ValueError("outtype found no match: %s", end)
    end = matchtypes[0]
    g = conversions_graph()
    return sorted(nx.all_simple_edge_paths(g, start, end, cutoff=cutoff), key=len)


def auto_pipeline(url: str, outtype: str, storage_options: dict | None = None) -> Pipeline:
    """Create pipeline from given URL to desired output type"""
    from intake.readers.datatypes import recommend

    if storage_options:
        data = recommend(url, storage_options=storage_options)[0](url=url, storage_options=storage_options)
    else:
        data = recommend(url)[0](url=url)
    start = data.qname()
    steps = path(start, outtype)
    reader = data.to_reader(outtype=steps[0][0][1])
    for s in steps[0][1:]:
        reader = reader.transform[s[1]]
    return reader
