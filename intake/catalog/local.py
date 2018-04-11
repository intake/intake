import glob
import inspect
import os
import os.path
import runpy

from ruamel_yaml import YAML
from ruamel_yaml.compat import StringIO
from ruamel_yaml.constructor import DuplicateKeyError

import jinja2
import pandas
import six
import shlex
import subprocess

from . import exceptions
from .entry import CatalogEntry
from ..source import registry as global_registry
from ..source.base import Plugin
from ..source.discovery import load_plugins_from_module


class TemplateContext(dict):
    """Collection of attributes and methods which are made available to a
    catalog's global namespace during template expansion.

    Usage:
        >>> import jinja2
        >>> context = TemplateContext('/some/dir')
        >>> jinja2.Template('The CATALOG_DIR variable is: {{ CATALOG_DIR }}'
        ... ).render(context)
        'The CATALOG_DIR variable is: /some/dir'
        >>> jinja2.Template('"ls" command results: {{ shell("ls /some/dir") }}'
        ... ).render(context)
        '"ls" command results: ["/some/dir/meta.txt", "/some/dir/test.dat"]'
        >>> jinja2.Template('"DBNAME" env var: {{ env("DBNAME") }}'
        ... ).render(context)
        '"DBNAME" env var: postgres'
    """
    def __init__(self, catalog_dir, shell_access=True, env_access=True,
                 run_local=True):
        """Constructor.

        Arguments:
            catalog_dir (str or None) :
                Value of the 'CATALOG_DIR', the location of the catalogue being
                evaluated. Should be None when handing local shell/env
                parameters, but using a remote server.
            shell_access (bool) :
                Default: True
                Whether the user has sufficient permissions to execute shell
                commands.
            env_access (bool) :
                Default: True
                Whether the user has sufficient permissions to read environment
                variables.
            run_local (bool):
                Whether
        """
        super(dict, self).__init__()
        if catalog_dir is not None and not isinstance(catalog_dir,
                                                      six.string_types):
            raise ValueError('catalog_dir argument must be a string.')
        self._shell_access = shell_access
        self._env_access = env_access
        self['CATALOG_DIR'] = catalog_dir
        self['shell'] = self.shell
        self['env'] = self.env

        self['localenv'] = self.localenv
        self['localshell'] = self.localshell

    def shell(self, cmd):
        """Return a list of strings, representing each line of stdout after
        executing ``cmd`` on the local machine or server. If the user does not
        have permission to execute shell commands, raise a PermissionsError.
        """
        if self['CATALOG_DIR'] is None:
            # pass-through shell commands to run on the remote server
            return cmd
        if not self._shell_access:
            raise exceptions.ShellPermissionDenied
        return subprocess.check_output(shlex.split(cmd),
                                       universal_newlines=True).strip().split()

    def env(self, env_var):
        """Return a string representing the state of environment variable
        ``env_var`` on the local machine or server. If the user does not have
        permission to read environment variables, raise a PermissionsError.
        """
        if self['CATALOG_DIR'] is None:
            # pass-through env lookup to the remote server
            return env_var
        if not self._env_access:
            raise exceptions.EnvironmentPermissionDenied
        return os.environ.get(env_var, '')

    def localshell(self, cmd):
        """Return a list of strings, representing each line of stdout after
        executing ``cmd`` on the local machine. Does not require permission.
        """
        return subprocess.check_output(shlex.split(cmd),
                                       universal_newlines=True).strip().split()

    def localenv(self, env_var):
        """Return a string representing the state of environment variable
        ``env_var`` on the local machine. Does not require permission.
        """
        return os.environ.get(env_var, '')


class TemplateStr(object):
    """A string-a-like that tags this string as being a Jinja template"""
    yaml_tag = '!template'

    def __init__(self, s):
        self._str = s
        self._template = jinja2.Template(s)

    def expand(self, context):
        return self._template.render(context)

    def __repr__(self):
        return 'TemplateStr(%s)' % repr(self._str)

    def __str__(self):
        return self._str

    def __eq__(self, other):
        if isinstance(other, TemplateStr):
            return self._str == other._str
        else:
            return False

    @classmethod
    def from_yaml(cls, loader, node):
        return TemplateStr(node.value)

    @classmethod
    def to_yaml(cls, dumper, data):
        return dumper.represent_scalar(cls.yaml_tag, data._str)


def expand_template(data, context):
    return data.expand(context) if isinstance(data, TemplateStr) else data


def expand_templates(args, template_context):
    expanded_args = {}

    for k, v in args.items():
        expanded_args[k] = expand_template(v, template_context)

    return expanded_args


class CatalogYAML(YAML):
    def dump(self, data, stream=None, **kwargs):
        """
        Output data to a given stream.

        If no stream is given, then the data is returned as a string.
        """
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kwargs)
        if inefficient:
            return stream.getvalue()


def yaml_instance():
    """Get a new YAML instance that supports templates"""
    yaml = CatalogYAML()
    yaml.register_class(TemplateStr)
    return yaml


class UserParameter(object):
    COERCION_RULES = {
        'bool': bool,
        'datetime': (lambda v=None: pandas.to_datetime(v)
                     if v else pandas.to_datetime(0)),
        'float': float,
        'int': int,
        'list': list,
        'str': str,
        'unicode': six.text_type
    }

    def __init__(self, name, description, type, default=None, min=None,
                 max=None, allowed=None):
        self.name = name
        self.description = description
        self.type = type
        self.default = default
        self.min = min
        self.max = max
        self.allowed = allowed

        self.default = UserParameter.coerce(self.type, self.default)

        if self.min:
            self.min = UserParameter.coerce(self.type, self.min)

        if self.max:
            self.max = UserParameter.coerce(self.type, self.max)

        if self.allowed:
            self.allowed = [UserParameter.coerce(self.type, item)
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

    @staticmethod
    def coerce(dtype, value):
        """
        Convert a value to a specific type.

        If the value is already the given type, then the original value is
        returned. If the value is None, then the default value given by the
        type constructor is returned. Otherwise, the type constructor converts
        and returns the value.
        """
        if type(value).__name__ == dtype:
            return value
        op = UserParameter.COERCION_RULES[dtype]
        return op() if value is None else op(value)

    def validate(self, value):
        value = UserParameter.coerce(self.type, value)

        if self.min is not None and value < self.min:
            raise ValueError('%s=%s is less than %s' % (self.name, value,
                                                        self.min))
        if self.max is not None and value > self.max:
            raise ValueError('%s=%s is greater than %s' % (self.name, value,
                                                           self.max))
        if self.allowed is not None and value not in self.allowed:
            raise ValueError('%s=%s is not one of the allowed values: %s'%
                             (self.name, value, ','.join(map(
                                 str, self.allowed))))

        return value


class LocalCatalogEntry(CatalogEntry):
    def __init__(self, name, description, driver, direct_access, args,
                 parameters, metadata, catalog_dir):
        self._name = name
        self._description = description
        self._driver = driver
        self._direct_access = direct_access
        self._open_args = args
        self._user_parameters = parameters
        self._metadata = metadata
        self._catalog_dir = catalog_dir
        self._plugin = None
        super(LocalCatalogEntry, self).__init__()

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
        params = TemplateContext(self._catalog_dir)
        for parameter in self._user_parameters:
            if parameter.name in user_parameters:
                params[parameter.name] = parameter.validate(
                    user_parameters[parameter.name])
            else:
                params[parameter.name] = parameter.default

        # FIXME: Check for unused user_parameters!

        open_args = expand_templates(self._open_args, template_context=params)
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
            except:
                pass

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


def get_line_column(obj, key=None):
    """Retrieve line/column from internal ruamel structure."""
    line, col = obj.lc.key(key) if key else (obj.lc.line, obj.lc.col)
    return line + 1, col + 1


class CatalogParser(object):
    def __init__(self, data, context=None):
        self._context = context if context else {}
        self._errors = []
        self._warnings = []
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
        line, col = get_line_column(obj, key)
        self._errors.append((line, col, msg))

    def warning(self, msg, obj, key=None):
        line, col = get_line_column(obj, key)
        self._warnings.append((line, col, msg))

    def _parse_plugin(self, obj, key):
        if not isinstance(obj[key], (str, TemplateStr)):
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
        valid_types = list(UserParameter.COERCION_RULES)

        params = {
            'name':name,
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
        ds = {}

        ds['name'] = name
        ds['description'] = self._getitem(data, 'description', str, required=False)
        ds['driver'] = self._getitem(data, 'driver', str)
        ds['direct_access'] = self._getitem(data, 'direct_access', str, required=False, default='forbid', choices=['forbid', 'allow', 'force'])
        ds['args'] = self._getitem(data, 'args', dict, required=False)
        ds['metadata'] = self._getitem(data, 'metadata', dict, required=False)

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

        return LocalCatalogEntry(catalog_dir=self._context['root'], **ds)

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
            data_sources=self._parse_data_sources(data))


class CatalogConfig(object):
    def __init__(self, path):
        self._path = path
        self._name = os.path.splitext(os.path.basename(
            self._path))[0].replace('.', '_')
        self._dir = os.path.dirname(os.path.abspath(self._path))

        # First, we load from YAML, failing if syntax errors are found
        yaml = yaml_instance()
        with open(self._path, 'r') as f:
            try:
                data = yaml.load(f.read())
            except DuplicateKeyError as e:
                # Wrap internal exception with our own exception
                raise exceptions.DuplicateKeyError(e)

        # Second, we validate the schema and semantics
        context = dict(root=self._dir)
        result = CatalogParser(data, context=context)
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
            ps.source = expand_template(ps.source, params)
            self._plugins.update(ps.load())

        self._entries = {}
        for entry in cfg['data_sources']:
            entry.find_plugin(self._plugins)
            self._entries[entry.name] = entry

    @property
    def name(self):
        return self._name

    @property
    def plugins(self):
        return self._plugins

    @property
    def entries(self):
        return self._entries
