#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import logging
import os
import posixpath
import yaml

from jinja2 import Template
import six
from dask.bytes import open_files

from intake.catalog.exceptions import DuplicateKeyError
from .. import __version__
from .base import Catalog
from . import exceptions
from .entry import CatalogEntry
from ..source import registry as global_registry
from ..source import get_plugin_class
from ..source.discovery import load_plugins_from_module
from ..utils import make_path_posix
from .utils import expand_templates, expand_defaults, coerce, COERCION_RULES


logger = logging.getLogger('intake')


class UserParameter(object):
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
        one of list``(COERSION_RULES)``
    default: type value
        same type as ``type``. It a str, may include special functions
        env, shell, client_env, client_shell.
    min, max: type value
        for validation of user input
    allowed: list of type
        for validation of user input
    """
    def __init__(self, name, description, type, default=None, min=None,
                 max=None, allowed=None):
        self.name = name
        self.description = description
        self.type = type
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

        if self.allowed:
            self.allowed = [coerce(self.type, item)
                            for item in self.allowed]

    def describe(self):
        """Information about this parameter"""
        desc = {
            'name': self.name,
            'description': self.description,
            'type': self.type,
            'default': self.default
        }
        for attr in ['min', 'max', 'allowed']:
            v = getattr(self, attr)
            if v is not None:
                desc[attr] = v
        return desc

    def expand_defaults(self, client=False, getenv=True, getshell=True):
        """Compile env, client_env, shell and client_shell commands
        """
        if not isinstance(self._default, six.string_types):
            return
        self.expanded_default = coerce(self.type, expand_defaults(
            self._default, client, getenv, getshell))

    def validate(self, value):
        """Does value meet parameter requirements?"""
        value = coerce(self.type, value)

        if self.min is not None and value < self.min:
            raise ValueError('%s=%s is less than %s' % (self.name, value,
                                                        self.min))
        if self.max is not None and value > self.max:
            raise ValueError('%s=%s is greater than %s' % (
                self.name, value, self.max))
        if self.allowed is not None and value not in self.allowed:
            raise ValueError('%s=%s is not one of the allowed values: %s' % (
                self.name, value, ','.join(map(str, self.allowed))))

        return value


class LocalCatalogEntry(CatalogEntry):
    """A catalog entry on the local system
    """
    def __init__(self, name, description, driver, direct_access, args, cache,
                 parameters, metadata, catalog_dir, getenv=True,
                 getshell=True, catalog=None):
        """

        Parameters
        ----------
        name: str
            How this entry is known, normally from its key in a YAML file
        description: str
            Brief text about the target source
        driver: str
            Name of the plugin that can load this
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
        self._description = description
        self._driver = driver
        self._direct_access = direct_access
        self._open_args = args
        self._cache = cache
        self._user_parameters = parameters
        self._metadata = metadata or {}
        self._catalog_dir = catalog_dir
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
            self._plugin = {d: get_plugin_class(driver[d]['class'])
                            for d in driver}
            self._plugin = {k: v for k, v in self._plugin.items()
                            if v is not None}
            containers = set(p.container for p in self._plugin.values())
        if len(containers) > 1:
            # this is an error, because cat is poorly specified, even if other
            # plugins are OK
            raise ValueError('Plugins for a data source must have only one '
                             'container.')
        if len(containers) == 0:
            # this is only debug, this single entry won't work, but cat is OK.
            # you get an error if you try to actually use this entry
            logger.debug('No plugins for entry: %s' % self.name)
            containers = [None]
        self._container = list(containers)[0]
        super(LocalCatalogEntry, self).__init__(
            getenv=getenv, getshell=getshell)

    @property
    def name(self):
        return self._name

    def describe(self):
        """Basic information about this entry"""
        return {
            'container': self._container,
            'description': self._description,
            'direct_access': self._direct_access,
            'user_parameters': [u.describe() for u in self._user_parameters]
        }

    def _create_open_args(self, user_parameters):
        params = {'CATALOG_DIR': self._catalog_dir}
        for parameter in self._user_parameters:
            if (parameter.name in user_parameters
                    and user_parameters[parameter.name] != parameter.default):
                params[parameter.name] = parameter.validate(
                    user_parameters[parameter.name])
            else:
                parameter.expand_defaults(getenv=self.getenv,
                                          getshell=self.getshell)
                params[parameter.name] = parameter.expanded_default

        self._open_args['cache'] = self._cache
        open_args = expand_templates(self._open_args, params)
        self._metadata['cache'] = open_args.pop('cache', [])
        md = self._metadata.copy() if self._metadata is not None else {}
        md['catalog_dir'] = self._catalog_dir
        open_args['metadata'] = md

        return open_args

    def describe_open(self, **user_parameters):
        return {
            'plugin': self._driver,
            'description': self._description,
            'direct_access': self._direct_access,
            'metadata': self._metadata,
            'args': self._create_open_args(user_parameters)
        }

    def get(self, **user_parameters):
        """Instantiate the DataSource for the given parameters"""
        open_args = self._create_open_args(user_parameters)
        plugin = user_parameters.get('plugin', None)
        if len(self._plugin) == 0:
            raise ValueError('No plugins loaded for this entry: %s'
                             % self._driver)
        elif isinstance(self._plugin, list):
            plugin = self._plugin[0]
        else:
            # dict
            if plugin is None:
                # default selection for dict
                plugin = list(self._plugin)[0]
            spec = self._driver[plugin]
            open_args.update(spec.get('args', {}))
            try:
                plugin = self._plugin[plugin]
            except KeyError:
                raise ValueError('Attempt to select not available plugin %s, '
                                 'perhaps import of plugin failed' % plugin)

        data_source = plugin(**open_args)
        data_source.catalog_object = self._catalog
        data_source.name = self.name
        data_source.description = self._description

        return data_source


def no_duplicates_constructor(loader, node, deep=False):
    """Check for duplicate keys while loading YAML

    https://gist.github.com/pypt/94d747fe5180851196eb
    """

    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping:
            raise DuplicateKeyError("while constructing a mapping",
                                    node.start_mark,
                                    "found duplicate key (%s)" % key,
                                    key_node.start_mark)
        mapping[key] = value

    return loader.construct_mapping(node, deep)


yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                     no_duplicates_constructor)


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

        if 'plugins' not in data:
            return

        if not isinstance(data['plugins'], dict):
            self.error("value of key 'plugins' must be a dictionary", data,
                       'plugins')
            return

        if 'source' not in data['plugins']:
            self.error("missing key 'source'", data['plugins'])
            return

        if not isinstance(data['plugins']['source'], list):
            self.error("value of key 'source' must be a list", data['plugins'],
                       'source')
            return

        for plugin_source in data['plugins']['source']:
            if not isinstance(plugin_source, dict):
                self.error("value in list of plugins sources must be a "
                           "dictionary", data['plugins'], 'source')
                continue

            if 'module' in plugin_source and 'dir' in plugin_source:
                self.error("keys 'module' and 'dir' both exist (select only "
                           "one)", plugin_source)
            elif 'module' in plugin_source:
                register_plugin_module(plugin_source['module'])
            elif 'dir' in plugin_source:
                path = Template(plugin_source['dir']).render(
                    {'CATALOG_DIR': self._context['root']})
                register_plugin_dir(path)
            else:
                self.error("missing one of the available keys ('module' or "
                           "'dir')", plugin_source)

    def _getitem(self, obj, key, dtype, required=True, default=None,
                 choices=None):
        if key in obj:
            if isinstance(obj[key], dtype):
                if choices and obj[key] not in choices:
                    self.error("value '{}' is invalid (choose from {})".format(
                        obj[key], choices), obj, key)
                else:
                    return obj[key]
            else:
                self.error("value '{}' is not expected type '{}'".format(
                    obj[key], dtype.__name__), obj, key)
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
            'name': name,
            'description': self._getitem(data, 'description', str),
            'type': self._getitem(data, 'type', str, choices=valid_types),
            'default': self._getitem(data, 'default', object),
            'min': self._getitem(data, 'min', object, required=False),
            'max': self._getitem(data, 'max', object, required=False),
            'allowed': self._getitem(data, 'allowed', object, required=False)
        }

        if params['description'] is None or params['type'] is None:
            return None

        return UserParameter(**params)

    def _parse_data_source(self, name, data):
        ds = {
            'name': name,
            'description': self._getitem(data, 'description', str,
                                         required=False),
            'driver': self._getitem(data, 'driver', object),
            'direct_access': self._getitem(
                data, 'direct_access', str, required=False, default='forbid',
                choices=['forbid', 'allow', 'force']),
            'args': self._getitem(data, 'args', dict, required=False),
            'cache': self._getitem(data, 'cache', list, required=False),
            'metadata': self._getitem(data, 'metadata', dict, required=False)
        }

        if ds['driver'] is None:
            return None

        ds['parameters'] = []

        if 'parameters' in data:
            if isinstance(data['parameters'], list):
                raise exceptions.ObsoleteParameterError

            if not isinstance(data['parameters'], dict):
                self.error("value of key 'parameters' must be a dictionary",
                           data, 'parameters')
                return None

            for name, parameter in data['parameters'].items():
                if not isinstance(name, str):
                    self.error("key '{}' must be a string".format(name),
                               data['parameters'], name)
                    continue

                if not isinstance(parameter, dict):
                    self.error("value of key '{}' must be a dictionary"
                               "".format(name), data['parameters'], name)
                    continue

                obj = self._parse_user_parameter(name, parameter)
                if obj:
                    ds['parameters'].append(obj)

        return LocalCatalogEntry(catalog_dir=self._context['root'],
                                 getenv=self.getenv, getshell=self.getshell,
                                 **ds)

    def _parse_data_sources(self, data):
        sources = []

        if 'sources' not in data:
            self.error("missing key 'sources'", data)
            return sources

        if isinstance(data['sources'], list):
            raise exceptions.ObsoleteDataSourceError

        if not isinstance(data['sources'], dict):
            self.error("value of key 'sources' must be a dictionary", data,
                       'sources')
            return sources

        for name, source in data['sources'].items():
            if not isinstance(name, str):
                self.error("key '{}' must be a string".format(name),
                           data['sources'], name)
                continue

            if not isinstance(source, dict):
                self.error("value of key '{}' must be a dictionary"
                           "".format(name), data['sources'], name)
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
            metadata=data.get('metadata', {})
        )


def register_plugin_module(mod):
    """Find plugins in given module"""
    for k, v in load_plugins_from_module(mod).items():
        if k:
            if isinstance(k, (list, tuple)):
                k = k[0]
            global_registry[k] = v


def register_plugin_dir(path):
    """Find plugins in given directory"""
    import glob
    for f in glob.glob(path + '/*.py'):
        for k, v in load_plugins_from_module(f).items():
            if k:
                global_registry[k] = v


def get_dir(path):
    if '://' in path:
        return posixpath.dirname(path)
    path = make_path_posix(
        os.path.join(os.getcwd(), os.path.dirname(path)))
    if path[-1] != '/':
        path += '/'
    return path


class YAMLFileCatalog(Catalog):
    """Catalog as described by a single YAML file"""
    version = __version__
    container = 'catalog'
    partition_access = None
    name = 'yaml_file_cat'

    def __init__(self, path, **kwargs):
        """
        Parameters
        ----------
        path: str
            Location of the file to parse (can be remote)
        """
        self.path = path
        super(YAMLFileCatalog, self).__init__(**kwargs)

    def _load(self):
        # First, we load from YAML, failing if syntax errors are found
        options = self.storage_options or {}
        if hasattr(self.path, 'path') or hasattr(self.path, 'read'):
            file_open = self.path
            self.path = make_path_posix(
                getattr(self.path, 'path',
                        getattr(self.path, 'name', 'file')))
        else:
            file_open = open_files(self.path, mode='rb', **options)
            assert len(file_open) == 1
            file_open = file_open[0]
        self.name = os.path.splitext(os.path.basename(
            self.path))[0].replace('.', '_')
        self._dir = get_dir(self.path)

        with file_open as f:
            text = f.read().decode()
        if "!template " in text:
            logger.warning("Use of '!template' deprecated - fixing")
            text = text.replace('!template ', '')

        data = yaml.load(text)

        if data is None:
            raise exceptions.CatalogException('No YAML data in file')

        # Second, we validate the schema and semantics
        context = dict(root=self._dir)
        result = CatalogParser(data, context=context, getenv=self.getenv,
                               getshell=self.getshell)
        if result.errors:
            raise exceptions.ValidationError(
                "Catalog '{}' has validation errors:\n\n{}"
                "".format(self.path, "\n".join(result.errors)), result.errors)

        cfg = result.data

        self._entries = {}
        for entry in cfg['data_sources']:
            entry._catalog = self
            self._entries[entry.name] = entry

        self.metadata = cfg.get('metadata', {})


global_registry['yaml_file_cat'] = YAMLFileCatalog


class YAMLFilesCatalog(Catalog):
    """Catalog as described by a multiple YAML files"""
    version = __version__,
    container = 'catalog',
    partition_access = None
    name = 'yaml_files_cat'

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
        super(YAMLFilesCatalog, self).__init__(**kwargs)

    def _load(self):
        # initial: find cat files
        # if flattening, need to get all entries from each.
        self._entries.clear()
        options = self.storage_options or {}
        if isinstance(self.path, (list, tuple)):
            files = sum([open_files(p, mode='rb', **options)
                         for p in self.path], [])
            self.name = "%i files" % len(files)
            self.path = [make_path_posix(p) for p in self.path]
        else:
            if isinstance(self.path, str) and '*' not in self.path:
                self.path = self.path + '/*'
            files = open_files(self.path, mode='rb', **options)
            self.path = make_path_posix(self.path)
            self.name = self.path
        if not set(f.path for f in files) == set(
                f.path for f in self._cat_files):
            # glob changed, reload all
            self._cat_files = files
            self._cats.clear()
        for f in files:
            name = os.path.split(f.path)[-1].replace(
                '.yaml', '').replace('.yml', '')
            kwargs = self.kwargs.copy()
            kwargs['path'] = f.path
            d = make_path_posix(os.path.dirname(f.path))
            if f.path not in self._cats:
                entry = LocalCatalogEntry(name, "YAML file: %s" % name,
                                          'yaml_file_cat', True,
                                          kwargs, [], {}, self.metadata, d)
                if self._flatten:
                    # store a concrete Catalog
                    try:
                        self._cats[f.path] = entry()
                    except IOError as e:
                        logger.info('Loading "%s" as a catalog failed: %s'
                                    '' % (entry, e))
                else:
                    # store a catalog entry
                    self._cats[f.path] = entry
        for name, entry in list(self._cats.items()):
            if self._flatten:
                entry.reload()
                inter = set(entry._entries).intersection(self._entries)
                if inter:
                    raise ValueError(
                        'Conflicting names when flattening multiple'
                        ' catalogs. Sources %s exist in more than'
                        ' one' % inter)
                self._entries.update(entry._entries)
            else:
                self._entries[entry._name] = entry


global_registry['yaml_files_cat'] = YAMLFilesCatalog
