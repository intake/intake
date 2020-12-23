from . import import_name
from .base import DataSource, Schema


class AliasSource(DataSource):
    """Refer to another named source, unmodified

    The purpose of an Alias is to be able to refer to other source(s) in the
    same catalog, perhaps leaving the choice of which target to load up to the
    user. This source makes no sense outside of a catalog.

    In this case, the output of the target source is not modified, but this
    class acts as a prototype 'derived' source for processing the output of
    some standard driver.

    After initial discovery, the source's container and other details will be
    updated from the target; initially, the AliasSource container is not
    any standard.
    """
    container = 'other'
    version = 1
    name = 'alias'

    def __init__(self, target, mapping=None, metadata=None, **kwargs):
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
        """
        super(AliasSource, self).__init__(metadata)
        self.target = target
        self.mapping = mapping or {target: target}
        self.kwargs = kwargs
        self.metadata = metadata
        self.source = None

    def _get_source(self):
        if self.cat is None:
            raise ValueError('AliasSource cannot be used outside a catalog')
        if self.source is None:
            self.source = self.cat[self.mapping[self.target]](
                metadata=self.metadata, **self.kwargs)
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


def first(targets, cat, kwargs):
    targ = targets[0]
    if cat:
        s = cat[targ]
        if kwargs and targ in kwargs:
            s = kwargs.configure_new(**kwargs[targ])
        return s
    else:
        return targ


def identity(x):
    return x


def first_discoverable(targets, cat, kwargs):
    for t in targets:
        try:
            if cat:
                s = cat[t]
                if kwargs and t in kwargs:
                    s = s.configure_new(**kwargs[t])
            else:
                s = t
            s.discover()
            return s
        except Exception:
            pass
    raise RuntimeError("No targets succeeded at discover()")


class DerivedSource(DataSource):
    input_container = "other"  # no constraint
    container = 'other'  # to be filled in per instance at access time
    name = 'derived'
    required_params = []  # list of kwargs that must be present
    optional_params = {}  # optional kwargs with defaults

    def __init__(self, targets, target_chooser=first, target_kwargs=None,
                 container=None, metadata=None, **kwargs):
        """

        Parameters
        ----------
        targets: list of string or DataSources
            If string(s), refer to entries of the same catalog as this Source
        target_chooser: function to choose between targets
            function(targets, cat) -> source, or a fully-qualified dotted string pointing
            to it
        target_kwargs: dict of dict with keys matching items of targets
        container: str (optional)
            Assumed output container, if different from input
        """
        self.targets = targets
        self._chooser = (target_chooser if callable(target_chooser)
                         else import_name(target_chooser))
        self._kwargs = target_kwargs
        self._source = None
        self._params = kwargs
        if container:
            self.container = container
        self._validate_params()
        super().__init__(metadata=metadata)

    def _validate_params(self):
        assert set(self.required_params) - set(self._params) == set()
        for par, val in self.optional_params.items():
            if par not in self._params:
                self._params[par] = val

    def _pick(self):
        self._source = self._chooser(self.targets, self.cat, self._kwargs)
        if self.input_container != "other":
            assert self._source.container == self.input_container

        self.metadata['target'] = self._source.metadata
        if self.container is None:
            self.container = self._source.container


class GenericTransform(DerivedSource):
    name = "transform"
    required_params = ["transform", "transform_kwargs"]
    optional_params = {"allow_dask": True}
    """
        transform: function to perform transform
            function(container_object) -> output, or a fully-qualified dotted string pointing
            to it
        transform_params: dict
            The keys are names of kwargs to pass to the transform function. Values are either
            concrete values to pass; or param objects which can be made into widgets (but
            must have a default value) - or a spec to be able to make these objects.
    """

    def _valdate_params(self):
        super()._validate_params()
        transform = self._params["transform"]
        self._transform = (transform if callable(transform)
                           else import_name(transform))

    def _get_schema(self):
        self._pick()
        return Schema()

    def to_dask(self):
        if not self._params['allow_dask']:
            raise ValueError("This transform is not compatible with Dask"
                             "because it has use_dask=False")
        return self._transform(self._source.to_dask(), **self._params["transform_kwargs"])

    def read(self):
        return self._transform(self._source.read(), **self._params["transform_kwargs"])


class Columns(DerivedSource):
    input_container = "dataframe"
    container = "dataframe"
    required_params = ["columns"]
    """
        columns: list
            Columns to choose from the target dataframe
    """

    def _get_schema(self):
        self._pick()
        disc = self._source.discover()
        self._dtypes = {k: v for k, v in disc['dtype'].items()
                        if k in self._params["columns"]}

        return Schema(dtype=self._dtypes,
                      shape=(None, len(self._dtypes)),
                      npartitions=self._source.npartitions,
                      metadata=self.metadata)

    def to_dask(self):
        self._pick()
        return self._source.to_dask()[self._params["columns"]]

    def read(self):
        return self.to_dask().compute()
