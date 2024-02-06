# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
"""
Base classes for Data Loader interface
"""

from yaml import dump

from ..utils import DictSerialiseMixin, pretty_describe


class Schema(dict):
    def __getattr__(self, item):
        return self[item]


class NoEntry(AttributeError):
    pass


class DataSourceBase(DictSerialiseMixin):
    """An object which can produce data

    This is the base class for all Intake plugins, including catalogs and
    remote (server) data objects. To produce a new plugin commonly involves
    subclassing this definition and overriding some or all of the methods.

    This class is not useful in itself, most methods raise NotImplemented.
    """

    name = None
    version = None
    container = None
    partition_access = False
    description = None
    dtype = None
    shape = None
    npartitions = 0
    _schema = None
    on_server = False
    cat = None  # the cat from which this source was made
    _entry = None
    metadata = {}

    def __init__(self, storage_options=None, metadata=None):
        # default data
        self.metadata = metadata or {}
        if isinstance(self.metadata, dict) and storage_options is None:
            storage_options = self._captured_init_kwargs.get("storage_options", {})
        self.storage_options = storage_options

    def _get_schema(self):
        """Subclasses should return an instance of base.Schema"""
        raise NotImplementedError

    def _get_partition(self, i):
        """Subclasses should return a container object for this partition

        This function will never be called with an out-of-range value for i.
        """
        raise NotImplementedError

    def __eq__(self, other):
        return (
            type(self) is type(other)
            and self._captured_init_args == other._captured_init_args
            and self._captured_init_kwargs == other._captured_init_kwargs
        )

    def __hash__(self):
        return hash((type(self), str(self._captured_init_args), str(self._captured_init_kwargs)))

    def _close(self):
        """Subclasses should close all open resources"""
        raise NotImplementedError

    def _load_metadata(self):
        """load metadata only if needed"""
        if self._schema is None:
            self._schema = self._get_schema()
            self.dtype = self._schema.dtype
            self.shape = self._schema.shape
            self.npartitions = self._schema.npartitions
            self.metadata.update(self._schema.extra_metadata)

    def _yaml(self):
        import inspect

        kwargs = self._captured_init_kwargs.copy()
        meta = kwargs.pop("metadata", self.metadata) or {}
        kwargs.update(
            dict(
                zip(
                    inspect.signature(self.__init__).parameters,
                    self._captured_init_args,
                )
            )
        )
        data = {
            "sources": {
                self.name: {
                    "driver": self.classname,
                    "description": self.description or "",
                    "metadata": meta,
                    "args": kwargs,
                }
            }
        }
        return data

    def yaml(self):
        """Return YAML representation of this data-source

        The output may be roughly appropriate for inclusion in a YAML
        catalog. This is a best-effort implementation
        """
        data = self._yaml()
        return dump(data, default_flow_style=False)

    def _ipython_display_(self):
        """Display the entry as a rich object in an IPython session."""
        from IPython.display import display

        data = self._yaml()["sources"]
        contents = dump(data, default_flow_style=False)
        display(
            {"application/yaml": contents, "text/plain": pretty_describe(contents)},
            metadata={"application/json": {"root": self.name}},
            raw=True,
        )

    def __repr__(self):
        return self.yaml()

    @property
    def is_persisted(self):
        """The base class does not interact with persistence"""
        return False

    @property
    def has_been_persisted(self):
        """The base class does not interact with persistence"""
        return False

    def _get_cache(self, urlpath):
        """The base class does not interact with caches"""
        return [urlpath]

    def discover(self):
        """Open resource and populate the source attributes."""
        self._load_metadata()

        return dict(
            dtype=self.dtype,
            shape=self.shape,
            npartitions=self.npartitions,
            metadata=self.metadata,
        )

    def read(self):
        """Load entire dataset into a container and return it"""
        if not self.partition_access or self.npartitions == 1:
            return self._get_partition(0)
        else:
            raise NotImplementedError

    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        self._load_metadata()
        for i in range(self.npartitions):
            yield self._get_partition(i)

    def read_partition(self, i):
        """Return a part of the data corresponding to i-th partition.

        By default, assumes i should be an integer between zero and npartitions;
        override for more complex indexing schemes.
        """
        self._load_metadata()
        if i < 0 or i >= self.npartitions:
            raise IndexError("%d is out of range" % i)

        return self._get_partition(i)

    def to_dask(self):
        """Return a dask container for this data source"""
        raise NotImplementedError

    def to_spark(self):
        """Provide an equivalent data object in Apache Spark

        The mapping of python-oriented data containers to Spark ones will be
        imperfect, and only a small number of drivers are expected to be able
        to produce Spark objects. The standard arguments may b translated,
        unsupported or ignored, depending on the specific driver.

        This method requires the package intake-spark
        """
        raise NotImplementedError

    @property
    def entry(self):
        if self._entry is None:
            raise NoEntry("Source was not made from a catalog entry")
        return self._entry

    def configure_new(self, **kwargs):
        """Create a new instance of this source with altered arguments

        Enables the picking of options and re-evaluating templates from any
        user-parameters associated with
        this source, or overriding any of the init arguments.

        Returns a new data source instance. The instance will be recreated from
        the original entry definition in a catalog **if** this source was originally
        created from a catalog.
        """
        if self._entry is not None:
            kw = {k: v for k, v in self._captured_init_kwargs.items() if k in self._passed_kwargs}
            kw.update(kwargs)
            obj = self._entry(**kw)
            obj._entry = self._entry
            return obj
        else:
            kw = self._captured_init_kwargs.copy()
            kw.update(kwargs)
            return type(self)(*self._captured_init_args, **kw)

    __call__ = get = configure_new  # compatibility aliases

    def describe(self):
        """Description from the entry spec"""
        return self.entry.describe()

    def close(self):
        """Close open resources corresponding to this data source."""
        self._close()

    # Boilerplate to make this object also act like a context manager
    def __enter__(self):
        self._load_metadata()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class DataSource(DataSourceBase):
    """A Data Source will all optional functionality

    When subclassed, child classes will have the base data source functionality,
    plus caching, plotting and persistence abilities.
    """

    pass


class PatternMixin:
    ...
