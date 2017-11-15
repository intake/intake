import glob
import importlib
import inspect
import os
import os.path
import runpy

try:
    from collections.abc import Sequence
except:
    from collections import Sequence

import jinja2
import yaml

from ..source.base import Plugin
from ..source import registry as global_registry


class TemplateStr(yaml.YAMLObject):
    '''A string-a-like that tags this string as being a Jinja template'''
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


yaml.SafeLoader.add_constructor('!template', TemplateStr.from_yaml)
yaml.SafeLoader.add_constructor(TemplateStr, TemplateStr.to_yaml)


class LocalCatalog(object):
    def __init__(self, filename):
        self._catalog_yaml_filename = os.path.abspath(filename)
        self._catalog_dir = os.path.dirname(self._catalog_yaml_filename)
        with open(filename, 'r') as f:
            catalog_yaml = yaml.safe_load(f.read())

        self.source_plugins = {}

        if 'plugins' in catalog_yaml:
            plugins_yaml = catalog_yaml['plugins']
            if 'source' in plugins_yaml:
                self.source_plugins = parse_source_plugins(plugins_yaml['source'], self._catalog_dir)

        self._entries = {
            key: parse_catalog_entry(value,
                                     catalog_plugin_registry=self.source_plugins,
                                     catalog_dir=self._catalog_dir)
            for key, value in catalog_yaml['sources'].items()
        }

    def list(self):
        return list(self._entries.keys())

    def describe(self, entry_name):
        return self._entries[entry_name].describe()

    def describe_open(self, entry_name, **user_parameters):
        return self._entries[entry_name].describe_open(**user_parameters)

    def get(self, entry_name, **user_parameters):
        return self._entries[entry_name].get(**user_parameters)

    def __str__(self):
        return '<Local Catalog: %s>' % self._catalog_yaml_filename


class ReloadableCatalog(object):
    '''A wrapper around local catalog that makes it easier to trigger a "reload" of it from disk'''
    def __init__(self, build_catalog_func):
        self._build_catalog_func = build_catalog_func
        self._catalog = self._build_catalog_func()

    def reload(self):
        # If this raises an exception, the current catalog will remain intact
        new_catalog = self._build_catalog_func()
        self._catalog = new_catalog

    @property
    def source_plugins(self):
        return self._catalog.source_plugins

    def list(self):
        return self._catalog.list()

    def describe(self, entry_name):
        return self._catalog.describe(entry_name)

    def describe_open(self, entry_name, **user_parameters):
        return self._catalog.describe_open(entry_name, **user_parameters)

    def get(self, entry_name, **user_parameters):
        return self._catalog.get(entry_name, **user_parameters)

    def __str__(self):
        return self._catalog.__str__()


class UnionCatalog(object):
    def __init__(self, catalogs):
        prefixed_catalogs = []
        for item in catalogs:
            if isinstance(item, Sequence):
                prefixed_catalogs.append(item)
            else:
                # implied empty prefix for catalog entries
                prefixed_catalogs.append(('', item))

        mappings = {}
        for prefix, catalog in prefixed_catalogs:
            for name in catalog.list():
                full_name = prefix + name
                if full_name in mappings:
                    # Collsion detected
                    other = mappings[full_name][0]
                    raise Exception('%s from %s duplicates name from %s' % (full_name, str(catalog), str(other)))
                mappings[full_name] = (catalog, name)

        # No plugins intrinsic to this catalog
        self.source_plugins = {}
        self._mappings = mappings

    def list(self):
        return list(self._mappings.keys())

    def describe(self, entry_name):
        cat, name = self._mappings[entry_name]
        return cat.describe(name)

    def describe_open(self, entry_name, **user_parameters):
        cat, name = self._mappings[entry_name]
        return cat.describe_open(name, **user_parameters)

    def get(self, entry_name, **user_parameters):
        cat, name = self._mappings[entry_name]
        return cat.get(name, **user_parameters)


class LocalCatalogEntry(object):
    def __init__(self, description, plugin, open_args, user_parameters, metadata, direct_access, catalog_dir):
        self._description = description
        self._plugin = plugin
        self._open_args = open_args
        self._user_parameters = user_parameters
        self._metadata = metadata
        self._direct_access = direct_access
        self._catalog_dir = catalog_dir

    def describe(self):
        return {
            'container': self._plugin.container,
            'description': self._description,
            'direct_access': self._direct_access,
            'user_parameters': [u.describe() for u in self._user_parameters.values()]
        }

    def _create_open_args(self, user_parameters):
        params = {'CATALOG_DIR': self._catalog_dir}
        for par_name, parameter in self._user_parameters.items():
            if par_name in user_parameters:
                params[par_name] = parameter.validate(user_parameters[par_name])
            else:
                params[par_name] = parameter.default

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


class UserParameter(object):
    def __init__(self, name, description, type, default, min=None, max=None, allowed=None):
        self.name = name
        self.description = description
        self.type = type
        self.default = default
        self.min = min
        self.max = max
        self.allowed = allowed

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

    def validate(self, value):
        if self.min is not None and value < self.min:
            raise ValueError('%s=%s is less than %s' % (self.name, value, self.min))
        if self.max is not None and value > self.max:
            raise ValueError('%s=%s is greater than %s' % (self.name, value, self.max))
        if self.allowed is not None and value not in self.allowed:
            raise ValueError('%s=%s is not one of the allowed values: %s' % (self.name, value, ','.join(map(str, self.allowed))))

        return value


def expand_templates(args, template_context):
    expanded_args = {}

    for k, v in args.items():
        if isinstance(v, TemplateStr):
            expanded_args[k] = v.expand(template_context)
        else:
            expanded_args[k] = v

    return expanded_args


def parse_catalog_entry(entry, catalog_plugin_registry, catalog_dir):
    description = entry.get('description', '')
    plugin_name = entry['driver']
    if plugin_name in catalog_plugin_registry:
        plugin = catalog_plugin_registry[plugin_name]
    else:
        plugin = global_registry[plugin_name]
    open_args = entry['args']
    metadata = entry.get('metadata', None)
    # This only matters when this entry is used by the catalog server
    direct_access = entry.get('direct_access', 'forbid')
    if direct_access not in ['forbid', 'allow', 'force']:
        raise ValueError('%s is not a valid value for direct_access')

    parameters = {}

    if 'parameters' in entry:
        for param_name, param_attrs in entry['parameters'].items():
            param_desc = param_attrs['description']
            param_type = param_attrs['type']
            # FIXME: Check for valid types
            param_default = param_attrs['default']

            # FIXME: Should coerce these values to parameter type
            param_min = param_attrs.get('min', None)
            param_max = param_attrs.get('max', None)
            param_allowed = param_attrs.get('allowed', None)
            parameters[param_name] = UserParameter(name=param_name,
                                                   description=param_desc,
                                                   type=param_type,
                                                   default=param_default,
                                                   min=param_min,
                                                   max=param_max,
                                                   allowed=param_allowed)

    return LocalCatalogEntry(description=description,
                             plugin=plugin,
                             open_args=open_args,
                             user_parameters=parameters,
                             direct_access=direct_access,
                             catalog_dir=catalog_dir,
                             metadata=metadata)


def parse_source_plugins(entry, catalog_dir):
    plugins = {}

    for item in entry:
        # Do jinja2 rendering of any template strings in this item
        item = expand_templates(item, dict(CATALOG_DIR=catalog_dir))

        if 'module' in item:
            plugins.update(load_from_module(item['module']))
        elif 'dir' in item:
            plugins.update(load_from_dir(item['dir']))
        else:
            raise ValueError('Incorrect plugin source syntax: %s' % (item,))

    return plugins


def load_from_module(module_str):
    plugins = {}

    mod = importlib.import_module(module_str)
    for _, cls in inspect.getmembers(mod, inspect.isclass):
        # Don't try to registry plugins imported into this module from somewhere else
        if issubclass(cls, Plugin) and cls.__module__ == module_str:
            p = cls()
            plugins[p.name] = p

    return plugins


def load_from_dir(dirname):
    plugins = {}
    pyfiles = glob.glob(os.path.join(dirname, '*.py'))

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
