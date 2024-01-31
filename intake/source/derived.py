from copy import deepcopy
from functools import lru_cache, partial
from textwrap import dedent

from .. import open_catalog
from ..catalog.exceptions import CatalogException
from . import import_name
from .base import DataSource, Schema


def _kwargs_string(kwargs_dict):
    return ", ".join([f"{k}={v}" for k, v in kwargs_dict.items()])


class PipelineStepError(CatalogException):
    pass


class MissingTargetError(CatalogException):
    def __init__(self, source, step_index, method, target):
        self.message = f"{source} step {step_index} {method}: {target} is not listed in the targets key of this pipeline."
        super().__init__(self.message)


cached_cats = lru_cache(10)(open_catalog)


def get_source(target, cat, kwargs, cat_kwargs):
    if ":" in target:
        caturl, target = target.rsplit(":", 1)
        cat = cached_cats(caturl, **cat_kwargs)
    if cat:
        return cat[target].configure_new(**kwargs)
    # for testing only
    return target  # pragma: no cover


class AliasSource(DataSource):
    """Refer to another named source, unmodified

    The purpose of an Alias is to be able to refer to other source(s) in the
    same catalog or an external catalog, perhaps leaving the choice of which
    target to load up to the user. This source makes no sense outside of a catalog.

    The "target" for an aliased data source will normally be a string. In the
    simple case, it is the name of a data source in the same catalog. However,
    we use the syntax "catalog:source" to refer to sources in other catalogs,
    where the part before ":" will be passed to intake.open_catalog,
    together with any keyword arguments from cat_kwargs.

    In this case, the output of the target source is not modified, but this
    class acts as a prototype 'derived' source for processing the output of
    some standard driver.

    After initial discovery, the source's container and other details will be
    updated from the target; initially, the AliasSource container is not
    any standard.
    """

    container = "other"
    version = 2
    name = "alias"

    def __init__(self, target, mapping=None, metadata=None, kwargs=None, cat_kwargs=None):
        """

        Parameters
        ----------
        target: str
            Name of the source to load, must be a key in the same catalog
        mapping: dict or None
            If given, use this to map the string passed as ``target`` to
            entries in the catalog
        metadata: dict or None
            Extra metadata to associate
        kwargs: passed on to the target
        cat_kwargs: passed on to the target catalog if target is in
            another catalog
        """
        super(AliasSource, self).__init__(metadata)
        self.target = target
        self.mapping = mapping or {target: target}
        self.kwargs = kwargs or {}
        self.cat_kwargs = cat_kwargs or {}
        self.metadata = metadata
        self.source = None

    def _get_source(self):
        if self.cat is None:
            raise ValueError("AliasSource cannot be used outside a catalog")
        if self.source is None:
            target = self.mapping[self.target]
            self.source = get_source(target, self.cat, self.kwargs, self.cat_kwargs)
            self.metadata = self.source.metadata.copy()
            self.container = self.source.container
            self.partition_access = self.source.partition_access
            self.description = self.source.description

    def discover(self):
        self._get_source()
        return self.source.discover()

    def read(self):
        self._get_source()
        return self.source.read()

    def read_partition(self, i):
        self._get_source()
        return self.source.read_partition(i)

    def read_chunked(self):
        self._get_source()
        return self.source.read_chunked()

    def to_dask(self):
        self._get_source()
        return self.source.to_dask()


def first(targets, cat, kwargs, cat_kwargs):
    """A target chooser that simply picks the first from the given list

    This is the default, particularly for the case of only one element in
    the list
    """
    targ = targets[0]
    return get_source(targ, cat, kwargs.get(targ, {}), cat_kwargs)


def first_discoverable(targets, cat, kwargs, cat_kwargs):
    """A target chooser: the first target for which discover() succeeds

    This may be useful where some drivers are not importable, or some
    sources can be available only sometimes.
    """
    for t in targets:
        try:
            s = get_source(t, cat, kwargs.get(t, {}), cat_kwargs)
            s.discover()
            return s
        except Exception:
            pass
    raise RuntimeError("No targets succeeded at discover()")


class DerivedSource(DataSource):
    """Base source deriving from another source in the same catalog

    Target picking and parameter validation are performed here, but
    you probably want to subclass from one of the more specific
    classes like ``DataFrameTransform``.
    """

    input_container = "other"  # no constraint
    container = "other"  # to be filled in per instance at access time
    required_params = []  # list of kwargs that must be present
    optional_params = {}  # optional kwargs with defaults

    def __init__(
        self,
        targets,
        target_chooser=first,
        target_kwargs=None,
        cat_kwargs=None,
        container=None,
        metadata=None,
        **kwargs,
    ):
        """

        Parameters
        ----------
        targets: list of string or DataSources
            If string(s), refer to entries of the same catalog as this Source
        target_chooser: function to choose between targets
            function(targets, cat) -> source, or a fully-qualified dotted string pointing
            to it
        target_kwargs: dict of dict with keys matching items of targets
        cat_kwargs: to pass to intake.open_catalog, if the target is in
            another catalog
        container: str (optional)
            Assumed output container, if known/different from input

        [Note: the exact form of target_kwargs and cat_kwargs may be
        subject to change]
        """
        self.targets = targets
        self._chooser = target_chooser if callable(target_chooser) else import_name(target_chooser)
        self._kwargs = target_kwargs or {}
        self._source = None
        self._params = kwargs
        self._cat_kwargs = cat_kwargs or {}
        if container:
            self.container = container
        self._validate_params()
        super().__init__(metadata=metadata)

    def _validate_params(self):
        """That all required params are present and that optional types match"""
        assert set(self.required_params) - set(self._params) == set()
        for par, val in self.optional_params.items():
            if par not in self._params:
                self._params[par] = val

    def _pick(self):
        """Pick the source from the given targets"""
        self._source = self._chooser(self.targets, self.cat, self._kwargs, self._cat_kwargs)
        if self.input_container != "other":
            assert self._source.container == self.input_container

        self.metadata["target"] = self._source.metadata
        if self.container is None:
            self.container = self._source.container


class GenericTransform(DerivedSource):
    required_params = ["transform", "transform_kwargs"]
    optional_params = {"allow_dask": True}
    """
    Perform an arbitrary function to transform an input

        transform: function to perform transform
            function(container_object) -> output, or a fully-qualified dotted string pointing
            to it
        transform_params: dict
            The keys are names of kwargs to pass to the transform function. Values are either
            concrete values to pass; or param objects which can be made into widgets (but
            must have a default value) - or a spec to be able to make these objects.
        allow_dask: bool (optional, default True)
            Whether to_dask() is expected to work, which will in turn call the
            target's to_dask()
    """

    def _validate_params(self):
        super()._validate_params()
        transform = self._params["transform"]
        self._transform = transform if callable(transform) else import_name(transform)

    def _get_schema(self):
        """We do not know the schema of a generic transform"""
        self._pick()
        return Schema()

    def to_dask(self):
        self._get_schema()
        if not self._params["allow_dask"]:
            raise ValueError(
                "This transform is not compatible with Dask" "because it has use_dask=False"
            )
        return self._transform(self._source.to_dask(), **self._params["transform_kwargs"])

    def read(self):
        self._get_schema()
        return self._transform(self._source.read(), **self._params["transform_kwargs"])


class DataFrameTransform(GenericTransform):
    """Transform where the input and output are both Dask-compatible dataframes

    This derives from GenericTransform, and you must supply ``transform`` and
    any ``transform_kwargs``.
    """

    input_container = "dataframe"
    container = "dataframe"
    optional_params = {}
    _df = None

    def to_dask(self):
        if self._df is None:
            self._pick()
            self._df = self._transform(self._source.to_dask(), **self._params["transform_kwargs"])
        return self._df

    def _get_schema(self):
        """load metadata only if needed"""
        self.to_dask()
        return Schema(
            dtype=self._df.dtypes,
            shape=(None, len(self._df.columns)),
            npartitions=self._df.npartitions,
            metadata=self.metadata,
        )

    def read(self):
        return self.to_dask().compute()


class Columns(DataFrameTransform):
    """Simple dataframe transform to pick columns

    Given as an example of how to make a specific dataframe transform.
    Note that you could use DataFrameTransform directly, by writing a
    function to choose the columns instead of a method as here.
    """

    input_container = "dataframe"
    container = "dataframe"
    required_params = ["columns"]

    def __init__(self, columns, **kwargs):
        """
        columns: list of labels (usually str) or slice
            Columns to choose from the target dataframe
        """
        # this class wants requires "columns", but DataFrameTransform
        # uses "transform_kwargs", which we don't need since we use a method for the
        # transform
        kwargs.update(transform=self.pick_columns, columns=columns, transform_kwargs={})
        super().__init__(**kwargs)

    def pick_columns(self, df):
        return df[self._params["columns"]]


class DataFramePipeline(DataFrameTransform):
    """Apply a sequence of transformations over a DataFrame

    The sequence of steps is defined as a list-of-dicts under
    the key "steps". Each step requires a "method" key and
    can optionally take "kwargs".

    loc/iloc are not supported, but query is recommended
    for filtering.

    A special method called 'cols' enables column selection
    and takes the kwarg 'columns', which can be a single value
    or a list of column names.

    merge, join, and assign will look for a source defined in
    the targets key of the pipeline.

    A special method called 'concat' will perform dd.concat
    operations where the list of dataframes to concat is
    provided in the kwarg 'dfs'.

    If a method cannot be found as an attribute of the output
    of the previous step, the pipeline will attempt to import
    the method and apply it.

    Here's an example of performing a groupby, selecting two
    columns and computing the mean.

    by_origin:
      driver: intake.source.derived.DataFramePipeline
      args:
        targets:
          - auto
        steps:
          - method: groupby
            kwargs:
              by: origin
          - method: cols
            kwargs:
              columns:
                - hp
                - mpg
          - method: mean
    """

    input_container = "dataframe"
    container = "dataframe"
    required_params = ["steps"]

    def __init__(self, steps, **kwargs):
        kwargs.update(transform=self.pipeline, steps=steps, transform_kwargs={})
        super().__init__(**kwargs)
        self._sources = {}

    def _get_sources(self):
        for target in self.targets:
            s = get_source(target, self.cat, self._kwargs, self._cat_kwargs).to_dask()
            self._sources[target] = s

    def pipeline(self, df):
        """Apply pipeline steps"""
        if not self._sources:
            self._get_sources()

        for idx, step in enumerate(self._params["steps"]):
            method = step["method"]
            kwargs = deepcopy(step.get("kwargs", {}))

            if callable(method):
                func = partial(method, df)

            elif method in ("loc", "iloc"):
                msg = dedent(
                    """\
                    iloc and loc are not supported. To select one or more columns use

                    - method: cols
                      kwargs:
                        columns: <single value or or list>

                    To filter use the the query method

                    - method: query
                      kwargs:
                        arg: A > 2
                      """
                )
                raise ValueError(msg)

            # this will catch accessor methods like .dt.<method> and .str.<method>
            elif method.count(".") == 1 and hasattr(df, method.split(".")[0]):
                sel, f = method.split(".")
                mod = getattr(df, sel)
                func = getattr(mod, f)

            elif method == "cols":
                columns = kwargs.pop("columns")
                func = partial(df.__getitem__, columns)

            elif method == "assign":
                to_assign = {}
                for col, value in kwargs.items():
                    if value in self.targets:
                        to_assign[col] = self._sources[value]
                    else:
                        to_assign[col] = value
                kwargs = to_assign
                func = df.assign

            elif method == "join":
                other = kwargs["other"]
                if not isinstance(other, list):
                    other = [other]

                kwargs["other"] = []
                for s in other:
                    if s in self.targets:
                        kwargs["other"].append(self._sources[s])
                    else:
                        raise MissingTargetError(self.name, idx + 1, method, s)
                func = df.join

            elif method == "merge":
                right = kwargs["right"]
                source = self._sources.get(right)
                if source is None:
                    raise MissingTargetError(self.name, idx + 1, method, right)
                else:
                    kwargs["right"] = source
                func = df.merge

            elif method in ("apply", "transform"):
                kwargs_func = kwargs.pop("func")
                f = kwargs_func if callable(kwargs_func) else import_name(kwargs_func)
                func = partial(getattr(df, method), f)

            elif method == "concat":
                objs = kwargs["dfs"]
                kwargs["dfs"] = [self._sources[s] for s in objs]
                func = import_name("dask.dataframe.concat")

            else:
                try:
                    func = getattr(df, method)
                except AttributeError:
                    func = partial(import_name(method), df)

            try:
                df = func(**kwargs)
            except Exception as e:
                original_kwargs = step.get("kwargs", {})
                s = _kwargs_string(original_kwargs)
                msg = dedent(
                    f"""\
                    DataFramePipeline source {self.name} step {idx+1} failed
                    {method}({s})

                    {repr(e)}
                    """
                )
                raise PipelineStepError(msg) from e

        return df
