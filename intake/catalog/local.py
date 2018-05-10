import glob
import inspect
import logging
import os
import os.path
import runpy
import yaml

from ruamel_yaml.constructor import DuplicateKeyError

from jinja2 import Template
import six
from dask.bytes import open_files

from . import exceptions
from .entry import CatalogEntry
from ..source import registry as global_registry
from ..source.base import Plugin
from ..source.discovery import load_plugins_from_module
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

        if self.min:
            self.min = coerce(self.type, self.min)

        if self.max:
            self.max = coerce(self.type, self.max)

        if self.allowed:
            self.allowed = [coerce(self.type, item)
                            for item in self.allowed]

    def describe(self):
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
        self.default = coerce(self.type, expand_defaults(
            self._default, client, getenv, getshell))

    def validate(self, value):
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
    def __init__(self, name, description, driver, direct_access, args,
                 parameters, metadata, catalog_dir, getenv=True, getshell=True):
        self._name = name
        self._description = description
        self._driver = driver
        self._direct_access = direct_access
        self._open_args = args
        self._user_parameters = parameters
        self._metadata = metadata
        self._catalog_dir = catalog_dir
        self._plugin = None
        super(LocalCatalogEntry, self).__init__(
            getenv=getenv, getshell=getshell)

    @property
    def name(self):
        return self._name

    def find_plugin(self, registry):
        if self._driver in registry:
            self._plugin = registry[self._driver]
        else:
            self._plugin = global_registry[self._driver]

    def describe(self):
        return {
            'container': self._plugin.container,
            'description': self._description,
            'direct_access': self._direct_access,
            'user_parameters': [u.describe() for u in self._user_parameters]
        }

    def _create_open_args(self, user_parameters):
        params = {'CATALOG_DIR': self._catalog_dir}
        for parameter in self._user_parameters:
            if parameter.name in user_parameters:
                params[parameter.name] = parameter.validate(
                    user_parameters[parameter.name])
            else:
                parameter.expand_defaults(getenv=self.getenv,
                                          getshell=self.getshell)
                params[parameter.name] = parameter.default

        open_args = expand_templates(self._open_args, params)
        open_args['metadata'] = self._metadata

        return open_args

    def describe_open(self, **user_parameters):
        return {
            'plugin': self._plugin.name,
            'description': self._description,
            'direct_access': self._direct_access,
            'metadata': self._metadata,
            'args': self._create_open_args(user_parameters)
        }

    def get(self, **user_parameters):
        open_args = self._create_open_args(user_parameters)
        data_source = self._plugin.open(**open_args)

        return data_source


class PluginSource(object):
    def __init__(self, type, source):
        self.type = type
        self.source = source

    def _load_from_module(self):
        return load_plugins_from_module(self.source)

    def _load_from_dir(self):
        plugins = {}
        pyfiles = glob.glob(os.path.join(self.source, '*.py'))

        for filename in pyfiles:
            try:
                globs = runpy.run_path(filename)
                for name, o in globs.items():
                    # Don't try to register plugins imported into this module
                    # from somewhere else
                    if inspect.isclass(o) and issubclass(
                            o, Plugin) and o.__module__ == '<run_path>':
                        p = o()
                        plugins[p.name] = p
                # If no exceptions, continue to next filename
                continue
            except Exception as ex:
                logger.warning('When importing {}:\n{}'.format(filename, ex))

            import imp
            base = os.path.splitext(filename)[0]
            mod = imp.load_source(base, filename)
            for name in mod.__dict__:
                obj = getattr(mod, name)
                # Don't try to register plugins imported into this module
                # from somewhere else
                if inspect.isclass(obj) and issubclass(
                        obj, Plugin) and obj.__module__ == base:
                    p = obj()
                    plugins[p.name] = p

        return plugins

    def load(self):
        if self.type == 'module':
            return self._load_from_module()
        elif self.type == 'dir':
            return self._load_from_dir()
        return {}


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

    def _parse_plugin(self, obj, key):
        if not isinstance(obj[key], six.string_types):
            self.error("value of key '{}' must be either be a string or "
                       "template".format(key), obj, key)
            return None

        extra_keys = set(obj) - set([key])
        for key in extra_keys:
            self.warning("key '{}' defined but not needed".format(key), key)

        return PluginSource(type=key, source=obj[key])

    def _parse_plugins(self, data):
        sources = []

        if 'plugins' not in data:
            return sources

        if not isinstance(data['plugins'], dict):
            self.error("value of key 'plugins' must be a dictionary", data,
                       'plugins')
            return sources

        if 'source' not in data['plugins']:
            self.error("missing key 'source'", data['plugins'])
            return sources

        if not isinstance(data['plugins']['source'], list):
            self.error("value of key 'source' must be a list", data['plugins'],
                       'source')
            return sources

        for plugin_source in data['plugins']['source']:
            if not isinstance(plugin_source, dict):
                self.error("value in list of plugins sources must be a "
                           "dictionary", data['plugins'], 'source')
                continue

            if 'module' in plugin_source and 'dir' in plugin_source:
                self.error("keys 'module' and 'dir' both exist (select only "
                           "one)", plugin_source)
            elif 'module' in plugin_source:
                obj = self._parse_plugin(plugin_source, 'module')
                if obj:
                    sources.append(obj)
            elif 'dir' in plugin_source:
                obj = self._parse_plugin(plugin_source, 'dir')
                if obj:
                    sources.append(obj)
            else:
                self.error("missing one of the available keys ('module' or "
                           "'dir')", plugin_source)

        return sources

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
            'default': self._getitem(data, 'default', object, required=False),
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
            'driver': self._getitem(data, 'driver', str),
            'direct_access': self._getitem(
                data, 'direct_access', str, required=False, default='forbid',
                choices=['forbid', 'allow', 'force']),
            'args': self._getitem(data, 'args', dict, required=False),
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


class CatalogConfig(object):
    def __init__(self, path, getenv=True, getshell=True, storage_options=None):
        self._path = path

        # First, we load from YAML, failing if syntax errors are found
        options = storage_options or {}
        if hasattr(path, 'path') or hasattr(path, 'read'):
            file_open = path
            self._path = getattr(path, 'path', getattr(path, 'name', 'file'))
        else:
            file_open = open_files(self._path, mode='rb', **options)
            assert len(file_open) == 1
            file_open = file_open[0]
        if file_open.path.startswith('http'):
            # do not reload from HTTP
            self.token = file_open.path
        else:
            self.token = file_open.fs.ukey(file_open.path)
        self._name = os.path.splitext(os.path.basename(
            self._path))[0].replace('.', '_')
        self._dir = os.path.dirname(self._path)
        with file_open as f:
            text = f.read().decode()
        if "!template " in text:
            logger.warning("Use of '!template' deprecated - fixing")
            text = text.replace('!template ', '')
        try:
            data = yaml.load(text)
        except DuplicateKeyError as e:
            # Wrap internal exception with our own exception
            raise exceptions.DuplicateKeyError(e)

        if data is None:
            raise exceptions.CatalogException('No YAML data in file')
        # Second, we validate the schema and semantics
        context = dict(root=self._dir)
        result = CatalogParser(data, context=context, getenv=getenv,
                               getshell=getshell)
        if result.errors:
            errors = ["line {}, column {}: {}".format(*error)
                      for error in result.errors]
            raise exceptions.ValidationError(
                "Catalog '{}' has validation errors:\n\n{}"
                "".format(path, "\n".join(errors)), result.errors)

        cfg = result.data

        # Finally, we create the plugins and entries. Failure is still possible.
        params = dict(CATALOG_DIR=self._dir)

        self._plugins = {}
        for ps in cfg['plugin_sources']:
            ps.source = Template(ps.source).render(params)
            self._plugins.update(ps.load())

        self._entries = {}
        for entry in cfg['data_sources']:
            entry.find_plugin(self._plugins)
            self._entries[entry.name] = entry

        self.metadata = cfg.get('metadata', {})

    @property
    def name(self):
        return self._name

    @property
    def plugins(self):
        return self._plugins

    @property
    def entries(self):
        return self._entries
