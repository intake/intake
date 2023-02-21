# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------
import collections
import inspect
import logging
import os
import warnings

import entrypoints
from fsspec import get_filesystem_class, open_files
from fsspec.core import split_protocol

from .. import __version__
from ..source import get_plugin_class, register_driver
from ..source.discovery import load_plugins_from_module
from ..utils import DictSerialiseMixin, classname, make_path_posix, yaml_load
from . import exceptions
from .base import Catalog, DataSource
from .entry import CatalogEntry
from .utils import COERCION_RULES, _has_catalog_dir, coerce, expand_defaults, merge_pars

logger = logging.getLogger("intake")


class UserParameter(DictSerialiseMixin):
    """
    A user-settable item that is passed to a DataSource upon instantiation.

    For string parameters, default may include special functions ``func(args)``,
    which *may* be expanded from environment variables or by executing a shell
    command.

    Parameters
    ----------
    name: str
        the key that appears in the DataSource argument strings
    description: str
        narrative text
    type: str
        one of list ``(COERSION_RULES)``
    default: type value
        same type as ``type``. It a str, may include special functions
        env, shell, client_env, client_shell.
    min, max: type value
        for validation of user input
    allowed: list of type
        for validation of user input
    """

    def __init__(self, name, description=None, type=None, default=None, min=None, max=None, allowed=None):
        self.name = name
        self.description = description
        self.type = type or __builtins__["type"](default).__name__
        self.min = min
        self.max = max
        self.allowed = allowed

        self._default = default
        try:
            self.default = coerce(self.type, default)
        except (ValueError, TypeError):
            self.default = None
        self.expanded_default = self.default

        if self.min:
            self.min = coerce(self.type, self.min)

        if self.max:
            self.max = coerce(self.type, self.max)

        if self.allowed and type != "mlist":
            self.allowed = [coerce(self.type, item) for item in self.allowed]

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name!r}>"

    __str__ = __repr__

    def describe(self):
        """Information about this parameter"""
        desc = {
            "name": self.name,
            "description": self.description,
            # the Parameter might not have a type at all
            "type": self.type or "unknown",
        }
        for attr in ["min", "max", "allowed", "default"]:
            v = getattr(self, attr)
            if v is not None:
                desc[attr] = v
        return desc

    def expand_defaults(self, client=False, getenv=True, getshell=True):
        """Compile env, client_env, shell and client_shell commands"""
        if not isinstance(self._default, str):
            self.expanded_default = self._default
        else:
            self.expanded_default = coerce(self.type, expand_defaults(self._default, client, getenv, getshell))

    def validate(self, value):
        """Does value meet parameter requirements?"""
        if self.type is not None:
            value = coerce(self.type, value)

        if self.type == "mlist":
            for v in value:
                if v not in self.allowed:
                    raise ValueError("Item %s not in allowed list", v)
            return value

        if self.min is not None and value < self.min:
            raise ValueError("%s=%s is less than %s" % (self.name, value, self.min))
        if self.max is not None and value > self.max:
            raise ValueError("%s=%s is greater than %s" % (self.name, value, self.max))
        if self.allowed is not None and value not in self.allowed:
            raise ValueError("%s=%s is not one of the allowed values: %s" % (self.name, value, ",".join(map(str, self.allowed))))

        return value


class LocalCatalogEntry(CatalogEntry):
    """A catalog entry on the local system"""

    def __init__(
        self, name, description, driver, direct_access=True, args={}, cache=[], parameters=[], metadata={}, catalog_dir="", getenv=True, getshell=True, catalog=None
    ):
        """

        Parameters
        ----------
        name: str
            How this entry is known, normally from its key in a YAML file, or
            if that is not provided then from name of file, or name of dir if
            file name is 'catalog.yaml' or 'catalog.yml'.
        description: str
            Brief text about the target source
        driver: str, list, dict or DataSource subclass
            The plugin(s) that can load this. Can be a simple name like "csv",
            which will be looked up in the registry, a fully-qualified class
            name ("package.mod.Class"), a list of these which would all work,
            a dictionary of the same with reasonable names, or an explicit
            class derived from DataSource.
        direct_access: bool
            Is the client allowed to attempt to reach this data
        args: dict
            Passed when instantiating the plugin DataSource
        parameters: list
            UserParameters that can be set
        metadata: dict
            Additional information about this data
        catalog_dir: str
            Location of the catalog, if known
        getenv: bool
            Can parameter default fields take values from the environment
        getshell: bool
            Can parameter default fields run shell commands
        catalog: bool
            Catalog object in which this entry belongs
        """
        self._name = name
        self._default_source = None
        self._description = description
        self._driver = driver
        self._direct_access = direct_access
        self._open_args = args
        self._cache = cache
        self._user_parameters = parameters
        self._metadata = metadata or {}
        self._catalog_dir = catalog_dir
        self._filesystem = None
        self._catalog = catalog
        if isinstance(driver, str):
            dr = get_plugin_class(driver)
            self._plugin = [dr] if dr is not None else []
            containers = set(p.container for p in self._plugin)
        elif isinstance(driver, list):
            self._plugin = [get_plugin_class(d) for d in driver]
            self._plugin = [p for p in self._plugin if p is not None]
            containers = set(p.container for p in self._plugin)
        elif isinstance(driver, dict):
            self._plugin = {d: get_plugin_class(driver[d]["class"]) for d in driver}
            self._plugin = {k: v for k, v in self._plugin.items() if v is not None}
            containers = set(p.container for p in self._plugin.values())
        elif inspect.isclass(driver) and issubclass(driver, DataSource):
            self._plugin = [driver]
            containers = {driver.container}
        else:
            raise TypeError("Driver was not a string, list, dict or DataSource:" " %s" % driver)
        if len(containers) > 1:
            # this is an error, because cat is poorly specified, even if other
            # plugins are OK
            raise ValueError("Plugins for a data source must have only one " "container.")
        if len(containers) == 0:
            # this is only debug, this single entry won't work, but cat is OK.
            # you get an error if you try to actually use this entry
            logger.debug("No plugins for entry: %s" % self.name)
            containers = [None]
        self._container = list(containers)[0]
        super(LocalCatalogEntry, self).__init__(getenv=getenv, getshell=getshell)

    @property
    def name(self):
        return self._name

    def describe(self):
        """Basic information about this entry"""
        if isinstance(self._plugin, list):
            pl = [p.name for p in self._plugin]
        elif isinstance(self._plugin, dict):
            pl = {k: classname(v) for k, v in self._plugin.items()}
        else:
            pl = self._plugin if isinstance(self._plugin, str) else self._plugin.name
        return {
            "name": self._name,
            "container": self._container,
            "plugin": pl,  # deprecated
            "driver": pl,
            "description": self._description,
            "direct_access": self._direct_access,
            "user_parameters": [u.describe() for u in self._user_parameters],
            "metadata": self._metadata,
            "args": self._open_args,
        }

    def _create_open_args(self, user_parameters):
        plugin = user_parameters.pop("plugin", None)

        md = self._metadata.copy() if self._metadata is not None else {}
        md["catalog_dir"] = self._catalog_dir
        if user_parameters.pop("cache", None) or self._cache:
            md["cache"] = user_parameters.pop("cache", None) or self._cache
        params = {
            "metadata": md,
            "CATALOG_DIR": self._catalog_dir,
        }
        params.update(self._open_args)
        if "storage_options" not in params and self._filesystem is not None and self._filesystem.storage_options and _has_catalog_dir(params):
            params["storage_options"] = self._filesystem.storage_options
        open_args = merge_pars(params, user_parameters, self._user_parameters, getshell=self.getshell, getenv=self.getenv, client=False)

        if len(self._plugin) == 0:
            raise ValueError(
                "No plugins loaded for this entry: %s\n"
                "A listing of installable plugins can be found "
                "at https://intake.readthedocs.io/en/latest/plugin"
                "-directory.html ." % self._driver
            )
        elif isinstance(self._plugin, list):
            plugin = self._plugin[0]
        else:
            # dict
            if plugin is None:
                # default selection for dict
                plugin = list(self._plugin)[0]
            spec = self._driver[plugin]
            open_args.update(spec.get("args", {}))
            try:
                plugin = self._plugin[plugin]
            except KeyError:
                raise ValueError("Attempt to select unavailable plugin %s, " "perhaps import of plugin failed" % plugin)
        return plugin, open_args

    def get(self, **user_parameters):
        """Instantiate the DataSource for the given parameters"""
        if not user_parameters and self._default_source is not None:
            return self._default_source

        plugin, open_args = self._create_open_args(user_parameters)
        data_source = plugin(**open_args)
        data_source.catalog_object = self._catalog
        data_source.name = self.name
        data_source.description = self._description
        data_source.cat = self._catalog

        # Cache the default source if there are no user parameters.
        if not user_parameters:
            self._default_source = data_source

        return data_source

    def clear_cached_default_source(self):
        """
        Clear a cached default source so it can be created anew (if, for instance,
        it depends on changing environment variables or execution context)
        """
        self._default_source = None


class CatalogParser(object):
    """Loads entries from a YAML spec"""

    def __init__(self, data, getenv=True, getshell=True, context=None):
        self._context = context if context else {}
        self._errors = []
        self._warnings = []
        self.getenv = getenv
        self.getshell = getshell
        self._data = self._parse(data)

    @property
    def ok(self):
        return len(self._errors) == 0

    @property
    def data(self):
        return self._data

    @property
    def errors(self):
        return self._errors

    @property
    def warnings(self):
        return self._warnings

    def error(self, msg, obj, key=None):
        if key is not None:
            self._errors.append(str((msg, obj, key)))
        else:
            self._errors.append(str((msg, obj)))

    def warning(self, msg, obj, key=None):
        if key is None:
            self._warnings.append(str((msg, obj)))
        else:
            self._warnings.append(str((msg, obj, key)))

    def _parse_plugins(self, data):
        if "plugins" not in data:
            return

        if not isinstance(data["plugins"], dict):
            self.error("value of key 'plugins' must be a dictionary", data, "plugins")
            return

        if "source" not in data["plugins"]:
            self.error("missing key 'source'", data["plugins"])
            return

        if not isinstance(data["plugins"]["source"], list):
            self.error("value of key 'source' must be a list", data["plugins"], "source")
            return

        for plugin_source in data["plugins"]["source"]:
            if not isinstance(plugin_source, dict):
                self.error("value in list of plugins sources must be a " "dictionary", data["plugins"], "source")
                continue

            if "module" in plugin_source:
                register_plugin_module(plugin_source["module"])
            elif "dir" in plugin_source:
                self.error(
                    "The key 'dir', and in general the feature of registering "
                    "plugins from a directory of Python scripts outside of "
                    "sys.path, is no longer supported. Use 'module'.",
                    plugin_source,
                )
            else:
                self.error("missing 'module'", plugin_source)

    def _getitem(self, obj, key, dtype, required=True, default=None, choices=None):
        if key in obj:
            if isinstance(obj[key], dtype):
                if choices and obj[key] not in choices:
                    self.error("value '{}' is invalid (choose from {})".format(obj[key], choices), obj, key)
                else:
                    return obj[key]
            else:
                self.error("value '{}' is not expected type '{}'".format(obj[key], dtype.__name__), obj, key)
            return None
        elif required:
            self.error("missing required key '{}'".format(key), obj)
            return None
        elif default:
            return default

        return None if dtype is object else dtype()

    def _parse_user_parameter(self, name, data):
        valid_types = list(COERCION_RULES)

        params = {
            "name": name,
            "description": self._getitem(data, "description", str),
            "type": self._getitem(data, "type", str, choices=valid_types),
            "default": self._getitem(data, "default", object, required=False),
            "min": self._getitem(data, "min", object, required=False),
            "max": self._getitem(data, "max", object, required=False),
            "allowed": self._getitem(data, "allowed", object, required=False),
        }

        if params["description"] is None or params["type"] is None:
            return None

        return UserParameter(**params)

    def _parse_data_source(self, name, data):
        if data.pop("remote", False):
            from intake.catalog.remote import RemoteCatalogEntry

            return RemoteCatalogEntry(getenv=self.getenv, getshell=self.getshell, **data)
        elif "cls" in data:
            from intake.utils import remake_instance

            return remake_instance(data)
        else:
            return self._parse_data_source_local(name, data)

    def _parse_data_source_local(self, name, data):
        ds = {
            "name": name,
            "description": self._getitem(data, "description", str, required=False),
            "driver": self._getitem(data, "driver", object),
            "direct_access": self._getitem(data, "direct_access", str, required=False, default="forbid", choices=["forbid", "allow", "force"]),
            "args": self._getitem(data, "args", dict, required=False),
            "cache": self._getitem(data, "cache", list, required=False),
            "metadata": self._getitem(data, "metadata", dict, required=False),
        }

        if ds["driver"] is None:
            return None

        ds["parameters"] = []

        if "parameters" in data:
            if isinstance(data["parameters"], list):
                raise exceptions.ObsoleteParameterError

            if not isinstance(data["parameters"], dict):
                self.error("value of key 'parameters' must be a dictionary", data, "parameters")
                return None

            for name, parameter in data["parameters"].items():
                if not isinstance(name, str):
                    self.error("key '{}' must be a string".format(name), data["parameters"], name)
                    continue

                if not isinstance(parameter, dict):
                    self.error("value of key '{}' must be a dictionary" "".format(name), data["parameters"], name)
                    continue

                obj = self._parse_user_parameter(name, parameter)
                if obj:
                    ds["parameters"].append(obj)

        return LocalCatalogEntry(catalog_dir=self._context["root"], getenv=self.getenv, getshell=self.getshell, **ds)

    def _parse_data_sources(self, data):
        sources = []

        if "sources" not in data:
            self.error("missing key 'sources'", data)
            return sources

        if isinstance(data["sources"], list):
            raise exceptions.ObsoleteDataSourceError

        if not isinstance(data["sources"], dict):
            self.error("value of key 'sources' must be a dictionary", data, "sources")
            return sources

        for name, source in data["sources"].items():
            if not isinstance(name, str):
                self.error("key '{}' must be a string".format(name), data["sources"], name)
                continue

            if not isinstance(source, dict):
                self.error("value of key '{}' must be a dictionary" "".format(name), data["sources"], name)
                continue

            obj = self._parse_data_source(name, source)
            if obj:
                sources.append(obj)

        return sources

    def _parse(self, data):
        if not isinstance(data, dict):
            self.error("catalog must be a dictionary", data)
            return

        return dict(
            plugin_sources=self._parse_plugins(data),
            data_sources=self._parse_data_sources(data),
            metadata=data.get("metadata", {}),
            name=data.get("name"),
            description=data.get("description"),
        )


def register_plugin_module(mod):
    """Find plugins in given module"""
    for k, v in load_plugins_from_module(mod).items():
        if k:
            if isinstance(k, (list, tuple)):
                k = k[0]
            # we clobber by default here
            register_driver(k, v, clobber=True)


def get_dir(path):
    if "://" in path:
        protocol, _ = split_protocol(path)
        out = get_filesystem_class(protocol)._parent(path)
        if "://" not in out:
            # some FSs strip this, some do not
            out = protocol + "://" + out
        return out
    path = make_path_posix(os.path.join(os.getcwd(), os.path.dirname(path)))
    if path[-1] != "/":
        path += "/"
    return path


class YAMLFileCatalog(Catalog):
    """Catalog as described by a single YAML file"""

    version = __version__
    container = "catalog"
    partition_access = None
    name = "yaml_file_cat"

    def __init__(self, path=None, text=None, autoreload=True, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the file to parse (can be remote)
        text: str (DEPRECATED)
            YAML contents of catalog, takes precedence over path
        autoreload : bool
            Whether to watch the source file for changes; make False if you want
            an editable Catalog
        """
        self.path = path
        if text is not None:
            logger.warning("YAMLFileCatalog `text` argument is deprecated")
            warnings.warn("`text` argument is deprecated", DeprecationWarning)
            self.parse(text)
            self.autoreload = False
        else:
            self.autoreload = autoreload  # set this to False if don't want reloads
        self.filesystem = kwargs.pop("fs", None)
        self.access = "name" not in kwargs
        super(YAMLFileCatalog, self).__init__(**kwargs)

    def _load(self, reload=False):
        """Load text of catalog file and pass to parse

        Will do nothing if auto-reload is off and reload is not explicitly
        requested
        """
        if self.access is False:
            # skip first load, if cat has given name (i.e., is subcat)
            self.updated = 0
            self.access = True
            return
        if self.autoreload or reload:
            # First, we load from YAML, failing if syntax errors are found
            options = self.storage_options or {}
            if hasattr(self.path, "path") or hasattr(self.path, "read"):
                file_open = self.path
                self.path = make_path_posix(getattr(self.path, "path", getattr(self.path, "name", "file")))
            elif self.filesystem is None:
                file_open = open_files(self.path, mode="rb", **options)
                assert len(file_open) == 1
                file_open = file_open[0]
                self.filesystem = file_open.fs
            else:
                file_open = self.filesystem.open(self.path, mode="rb")
            self._dir = get_dir(self.path)

            with file_open as f:
                text = f.read().decode()
            if "!template " in text:
                logger.warning("Use of '!template' deprecated - fixing")
                text = text.replace("!template ", "")
            self.parse(text)

    def add(self, source, name=None, path=None, storage_options=None):
        """Add sources to the catalog and save into the original file

        This adds the source into the catalog dictionary, and saves the
        resulting catalog as YAML. Typically, this would be used to update a
        catalog file in-place. Optionally, the new catalog can be saved to a
        new location, in which case the new catalog is returned.

        Note that if a source of the given name exists, it will be clobbered.

        Parameters
        ----------
        source : DataSource instance
            The source whose spec we want to save
        name : str or None
            The name the source is to have in the catalog; use the source's
            name attribute, if not given.
        path : str or None
            Location to save the new catalog; if None, the original location
            from which it was loaded
        storage_options : dict or None
            If saving to a new location, use these arguments for the filesystem
            backend

        Returns
        -------
        YAMLFileCatalog instance, containing the new entry
        """
        import yaml

        entries = self._entries.copy()
        name = name or source.name or "source"
        entries[name] = source

        if path is None:
            options = self.storage_options or {}
            file_open = open_files([self.path], mode="wt", **options)
        else:
            options = storage_options or {}
            file_open = open_files([path], mode="wt", **options)
        assert len(file_open) == 1
        file_open = file_open[0]

        data = {"metadata": self.metadata, "sources": {}}
        for e in entries:
            data["sources"][e] = list(entries[e]._yaml()["sources"].values())[0]
        with file_open as f:
            yaml.dump(data, f, default_flow_style=False)

        if path:
            return self
        else:
            return YAMLFileCatalog(self.path, storage_options=storage_options, autoreload=self.autoreload)

    def parse(self, text):
        """Create entries from catalog text

        Normally the text comes from the file at self.path via the ``_load()``
        method, but could be explicitly set instead. A copy of the text is
        kept in attribute ``.text`` .

        Parameters
        ----------
        text : str
            YAML formatted catalog spec
        """
        self.text = text
        data = yaml_load(self.text)

        if data is None:
            raise exceptions.CatalogException("No YAML data in file")

        # Second, we validate the schema and semantics
        context = dict(root=self._dir)
        result = CatalogParser(data, context=context, getenv=self.getenv, getshell=self.getshell)
        if result.errors:
            raise exceptions.ValidationError("Catalog '{}' has validation errors:\n\n{}" "".format(self.path, "\n".join(result.errors)), result.errors)

        cfg = result.data

        self._entries = {}
        shared_parameters = data.get("metadata", {}).get("parameters", {})
        self.user_parameters.update({name: UserParameter(name, **attrs) for name, attrs in shared_parameters.items()})

        for entry in cfg["data_sources"]:
            entry._catalog = self
            self._entries[entry.name] = entry
            entry._filesystem = self.filesystem

        meta = self.metadata.copy()
        meta.update(cfg.get("metadata", {}))
        self.metadata = meta
        self.name = self.name or cfg.get("name") or self.name_from_path
        self.description = self.description or cfg.get("description")

    @property
    def name_from_path(self):
        """If catalog is named 'catalog' take name from parent directory"""
        name = os.path.splitext(os.path.basename(self.path))[0]
        if name == "catalog":
            name = os.path.basename(os.path.dirname(self.path))
        return name.replace(".", "_")


class YAMLFilesCatalog(Catalog):
    """Catalog as described by a multiple YAML files"""

    version = (__version__,)
    container = "catalog"
    partition_access = None
    name = "yaml_files_cat"

    def __init__(self, path, flatten=True, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the files to parse (can be remote), including possible
            glob (*) character(s). Can also be list of paths, without glob
            characters.
        flatten: bool (True)
            Whether to list all entries in the cats at the top level (True)
            or create sub-cats from each file (False).
        """
        self.path = path
        self._flatten = flatten
        self._kwargs = kwargs.copy()
        self._cat_files = []
        self._cats = {}
        self.access = "name" not in kwargs
        super(YAMLFilesCatalog, self).__init__(**kwargs)

    def _load(self):
        # initial: find cat files
        # if flattening, need to get all entries from each.
        if self.access is False:
            # skip first load, if cat has given name (i.e., is subcat)
            self.updated = 0
            self.access = True
            return
        self._entries.clear()
        options = self.storage_options or {}
        if isinstance(self.path, (list, tuple)):
            files = sum([open_files(p, mode="rb", **options) for p in self.path], [])
            self.name = self.name or "%i files" % len(files)
            self.description = self.description or f"Catalog generated from {len(files)} files"
            self.path = [make_path_posix(p) for p in self.path]
        else:
            if isinstance(self.path, str) and "*" not in self.path:
                self.path = self.path + "/*"
            files = open_files(self.path, mode="rb", **options)
            self.path = make_path_posix(self.path)
            self.name = self.name or self.path
            self.description = self.description or f"Catalog generated from all files found in {self.path}"
        if not set(f.path for f in files) == set(f.path for f in self._cat_files):
            # glob changed, reload all
            self._cat_files = files
            self._cats.clear()
        for f in files:
            name = os.path.split(f.path)[-1].replace(".yaml", "").replace(".yml", "")
            kwargs = self.kwargs.copy()
            kwargs["path"] = f.path
            d = make_path_posix(os.path.dirname(f.path))
            if f.path not in self._cats:
                entry = LocalCatalogEntry(name, "YAML file: %s" % name, "yaml_file_cat", True, kwargs, [], [], self.metadata, d)
                if self._flatten:
                    # store a concrete Catalog
                    try:
                        cat = entry()
                        cat.reload()
                        self.user_parameters.update(cat.user_parameters)
                        self._cats[f.path] = cat
                    except IOError as e:
                        logger.info('Loading "%s" as a catalog failed: %s' "" % (entry, e))
                else:
                    # store a catalog entry
                    self._cats[f.path] = entry
        entries = {}
        for name, entry in list(self._cats.items()):
            if self._flatten:
                entry.reload()
                inter = set(entry._entries).intersection(entries)
                if inter:
                    raise ValueError("Conflicting names when flattening multiple" " catalogs. Sources %s exist in more than" " one" % inter)
                entries.update(entry._entries)
            else:
                entries[entry._name] = entry
        self._entries.update(entries)


class MergedCatalog(Catalog):
    """
    A Catalog that merges the entries of a list of catalogs.
    """

    def __init__(self, catalogs, *args, **kwargs):
        self._catalogs = catalogs
        super().__init__(*args, **kwargs)

    def _load(self):
        for catalog in self._catalogs:
            catalog._load()
        self._entries = collections.ChainMap(*(catalog._entries for catalog in self._catalogs))


class EntrypointEntry(CatalogEntry):
    """
    A catalog entry for an entrypoint.

    """

    def __init__(self, entrypoint):
        self._entrypoint = entrypoint
        self._container = None
        self._user_parameters = []
        super().__init__()

    def __repr__(self):
        return f"<Entry '{self.name}'>"

    @property
    def name(self):
        return self._entrypoint.name

    def describe(self):
        """Basic information about this entry"""
        if self._container is None:
            self._container = self().container
        return {
            "name": self.name,
            "module_name": self._entrypoint.module_name,
            "object_name": self._entrypoint.object_name,
            "distro": self._entrypoint.distro,
            "extras": self._entrypoint.extras,
            "container": self._container,
        }

    def get(self):
        """Instantiate the DataSource for the given parameters"""
        return self._entrypoint.load()


class EntrypointsCatalog(Catalog):
    """
    A catalog of discovered entrypoint catalogs.
    """

    def __init__(self, *args, entrypoints_group="intake.catalogs", paths=None, **kwargs):
        self._entrypoints_group = entrypoints_group
        self._paths = paths
        super().__init__(*args, **kwargs)

    def _load(self):
        catalogs = entrypoints.get_group_named(self._entrypoints_group, path=self._paths)
        self.name = self.name or "EntrypointsCatalog"
        self.description = self.description or f"EntrypointsCatalog of {len(catalogs)} catalogs."
        for name, entrypoint in catalogs.items():
            try:
                self._entries[name] = EntrypointEntry(entrypoint)
            except Exception as e:
                warnings.warn(f"Failed to load {name}, {entrypoint}, {e!r}.")


# Register these early in the import process to support the default catalog
# which is built at import time. (Without this, 'yaml_file_cat' is looked for
# in intake.registry before the registry has been populated.)
register_driver("yaml_file_cat", YAMLFileCatalog, clobber=True)
register_driver("yaml_files_cat", YAMLFilesCatalog, clobber=True)
