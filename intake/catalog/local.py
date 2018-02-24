import glob
import inspect
import os
import os.path
import runpy

from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO

import jinja2
import marshmallow
import pandas
import six

from .entry import CatalogEntry
from .utils import PermissionsError
from ..source.base import Plugin
from ..source import registry as global_registry
from ..source.discovery import load_plugins_from_module

try:
    import builtins
except:
    import __builtin__ as builtins


class TemplateContext(dict):
    """Collection of attributes and methods which are made available to a
    catalog's global namespace during template expansion.

    Usage:
        >> import jinja2
        >> context = TemplateContext('/some/dir')
        >> jinja2.Template('The CATALOG_DIR variable is: {{ CATALOG_DIR }}').render(context)
        'The CATALOG_DIR variable is: /some/dir'
        >> jinja2.Template('"ls" command results: {{ shell("ls /some/dir") }}').render(context)
        '"ls" command results: ["/some/dir/meta.txt", "/some/dir/test.dat"]'
        >> jinja2.Template('"DBNAME" env var: {{ env("DBNAME") }}').render(context)
        '"DBNAME" env var: postgres'
    """
    def __init__(self, catalog_dir, shell_access=True, env_access=True):
        """Constructor.

        Arguments:
            catalog_dir (str) :
                Value of the 'CATALOG_DIR' entry in ``self``.
            shell_access (bool) :
                Default: True
                Whether the user has sufficient permissions to execute shell
                commands.
            env_access (bool) :
                Default: True
                Whether the user has sufficient permissions to read environment
                variables.
        """
        super(dict, self).__init__()
        assert isinstance(catalog_dir, six.string_types), (
            'catalog_dir argument must be a string.'
        )
        self._shell_access = shell_access
        self._env_access = env_access
        self['CATALOG_DIR'] = catalog_dir
        self['shell'] = self.shell
        self['env'] = self.env

    def shell(self, cmd):
        """Return a list of strings, representing each line of stdout after
        executing ``cmd`` on the local machine. If the user does not have
        permission to execute shell commands, raise a PermissionsError.
        """
        if not self._shell_access:
            raise PermissionsError('Additional permissions needed to execute '
                                   'shell commands.')
        import shlex, subprocess
        return subprocess.check_output(shlex.split(cmd),
                                       universal_newlines=True).strip().split()

    def env(self, env_var):
        """Return a string representing the state of environment variable
        ``env_var`` on the local machine. If the user does not have permission
        to read environment variables, raise a PermissionsError.
        """
        if not self._env_access:
            raise PermissionsError('Additional permissions needed to read '
                                   'environment variables.')
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


def check_uniqueness(objs, key):
    values = [getattr(obj, key) for obj in objs]
    unique_values = set()
    for value in values:
        if value in unique_values:
            raise marshmallow.ValidationError("{} already exists: '{}'".format(key, value), key)
        else:
            unique_values.add(value)


class UserParameter(object):
    COERCION_RULES = {
        'bool': bool,
        'datetime': lambda v=None: pandas.to_datetime(v) if v else pandas.to_datetime(0),
        'float': float,
        'int': int,
        'list': list,
        'str': str,
        'unicode': getattr(builtins, 'unicode', str)
    }

    def __init__(self, name, description, type, default=None, min=None, max=None, allowed=None):
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
            self.allowed = [UserParameter.coerce(self.type, item) for item in self.allowed]

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
            raise ValueError('%s=%s is less than %s' % (self.name, value, self.min))
        if self.max is not None and value > self.max:
            raise ValueError('%s=%s is greater than %s' % (self.name, value, self.max))
        if self.allowed is not None and value not in self.allowed:
            raise ValueError('%s=%s is not one of the allowed values: %s' % (self.name, value, ','.join(map(str, self.allowed))))

        return value


class LocalCatalogEntry(CatalogEntry):
    def __init__(self, name, description, driver, direct_access, args, parameters, metadata, catalog_dir):
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
                params[parameter.name] = parameter.validate(user_parameters[parameter.name])
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
                globals = runpy.run_path(filename)
                for name, o in globals.items():
                    # Don't try to registry plugins imported into this module from somewhere else
                    if inspect.isclass(o) and issubclass(o, Plugin) and o.__module__ == '<run_path>':
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
                # Don't try to registry plugins imported into this module from somewhere else
                if inspect.isclass(obj) and issubclass(obj, Plugin) and obj.__module__ == base:
                    p = obj()
                    plugins[p.name] = p

        return plugins

    def load(self):
        if self.type == 'module':
            return self._load_from_module()
        elif self.type == 'dir':
            return self._load_from_dir()
        return {}


class UserParameterSchema(marshmallow.Schema):
    name = marshmallow.fields.String(required=True)
    description = marshmallow.fields.String(required=True)
    type = marshmallow.fields.String(required=True)
    default = marshmallow.fields.Raw(missing=None)
    min = marshmallow.fields.Raw(missing=None)
    max = marshmallow.fields.Raw(missing=None)
    allowed = marshmallow.fields.Raw(missing=None)

    @marshmallow.validates('type')
    def validate_type(self, data):
        marshmallow.validate.OneOf(list(UserParameter.COERCION_RULES))(data)

    @marshmallow.post_load
    def instantiate(self, params):
        return UserParameter(**params)


class LocalCatalogEntrySchema(marshmallow.Schema):
    name = marshmallow.fields.String(required=True)
    description = marshmallow.fields.String(missing='')
    driver = marshmallow.fields.String(required=True)
    direct_access = marshmallow.fields.String(missing='forbid')
    args = marshmallow.fields.Dict(missing={})
    metadata = marshmallow.fields.Dict(missing={})
    parameters = marshmallow.fields.Nested(UserParameterSchema, many=True, missing=[])

    @marshmallow.validates('direct_access')
    def validate_direct_access(self, data):
        marshmallow.validate.OneOf(['forbid', 'allow', 'force'])(data)

    @marshmallow.post_load
    def instantiate(self, data):
        check_uniqueness(data['parameters'], 'name')
        return LocalCatalogEntry(catalog_dir=self.context['root'], **data)


class PluginSourceSchema(marshmallow.Schema):
    type = marshmallow.fields.String(required=True)
    source = marshmallow.fields.Raw(required=True)

    @marshmallow.validates('type')
    def validate_type(self, data):
        marshmallow.validate.OneOf(['dir', 'module'])(data)

    @marshmallow.validates('source')
    def validate_source(self, data):
        if not isinstance(data, (str, TemplateStr)):
            raise marshmallow.ValidationError('Plugin source must be either be a string or template', 'source')

    @marshmallow.post_load
    def instantiate(self, data):
        return PluginSource(**data)


class CatalogConfigSchema(marshmallow.Schema):
    plugin_sources = marshmallow.fields.Nested(PluginSourceSchema, many=True, missing=[])
    data_sources = marshmallow.fields.Nested(LocalCatalogEntrySchema,
                                             many=True,
                                             missing=[],
                                             load_from='sources',
                                             dump_to='sources')

    @marshmallow.pre_load(pass_many=True)
    def transform(self, data, many):
        data['plugin_sources'] = []
        if 'plugins' in data:
            for obj in data['plugins']['source']:
                key = list(obj)[0]
                data['plugin_sources'].append(dict(type=key, source=obj[key]))
            del data['plugins']
        return data

    @marshmallow.post_load
    def validate(self, data):
        check_uniqueness(data['data_sources'], 'name')


class ValidationError(Exception):
    def __init__(self, message, errors):
        super(ValidationError, self).__init__(message)
        self.errors = errors


class CatalogConfig(object):
    def __init__(self, path):
        self._path = path
        self._name = os.path.splitext(os.path.basename(self._path))[0].replace('.', '_')
        self._dir = os.path.dirname(os.path.abspath(self._path))

        # First, we load from YAML, failing if syntax errors are found
        yaml = yaml_instance()
        with open(self._path, 'r') as f:
            data = yaml.load(f.read())

        # Second, we validate the schema and semantics
        context = dict(root=self._dir)
        schema = CatalogConfigSchema(context=context)
        result = schema.load(data)
        if result.errors:
            raise ValidationError("Catalog '{}' has validation errors: {}"
                                  "".format(path, result.errors), result.errors)

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
