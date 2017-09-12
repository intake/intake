import os
import importlib
import inspect
import glob
import runpy

import yaml
import jinja2

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

    @classmethod
    def from_yaml(cls, loader, node):
        return TemplateStr(node.value)

    @classmethod
    def to_yaml(cls, dumper, data):
        return dumper.represent_scalar(cls.yaml_tag, data._str)


yaml.SafeLoader.add_constructor('!template', TemplateStr.from_yaml)
yaml.SafeLoader.add_constructor(TemplateStr, TemplateStr.to_yaml)


class LocalCatalog:
    def __init__(self, filename):
        self._catalog_dir = os.path.dirname(os.path.abspath(filename))
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

    def get(self, entry_name, **user_parameters):
        return self._entries[entry_name].get(**user_parameters)


class LocalCatalogHandle:
    '''A wrapper around local catalog that makes it easier to trigger a "reload" of it from disk'''
    def __init__(self, filename):
        self._filename = filename

        self._catalog = LocalCatalog(self._filename)

    def reload(self):
        # If this raises an exception, the current catalog will remain intact
        new_catalog = LocalCatalog(self._filename)

        self._catalog = new_catalog

    @property
    def source_plugins(self):
        return self._catalog.source_plugins

    def list(self):
        return self._catalog.list()

    def describe(self, entry_name):
        return self._catalog.describe(entry_name)

    def get(self, entry_name, **user_parameters):
        return self._catalog.get(entry_name, **user_parameters)


class LocalCatalogEntry:
    def __init__(self, description, plugin, open_args, user_parameters, metadata, catalog_dir):
        self._description = description
        self._plugin = plugin
        self._open_args = open_args
        self._user_parameters = user_parameters
        self._metadata = metadata
        self._catalog_dir = catalog_dir

    def describe(self):
        return {
          'container': self._plugin.container,
          'description': self._description,
          'user_parameters': [u.describe() for u in self._user_parameters.values()]
        }

    def get(self, **user_parameters):
        params = { 'CATALOG_DIR': self._catalog_dir }
        for par_name, parameter in self._user_parameters.items():
            if par_name in user_parameters:
                params[par_name] = parameter.validate(user_parameters[par_name])
            else:
                params[par_name] = parameter.default

        # FIXME: Check for unused user_parameters!

        open_args = expand_templates(self._open_args, template_context=params)
        open_args['metadata'] = self._metadata

        data_source = self._plugin.open(**open_args)

        return data_source


class UserParameter:
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
            parameters[param_name] = UserParameter(name=param_name, description=param_desc, type=param_type, default=param_default,
                min=param_min, max=param_max, allowed=param_allowed)

    return LocalCatalogEntry(description=description, plugin=plugin,
        open_args=open_args, user_parameters=parameters,
        catalog_dir=catalog_dir, metadata=metadata)


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
        globals = runpy.run_path(filename)

        for name, o in globals.items():
            # Don't try to registry plugins imported into this module from somewhere else
            if inspect.isclass(o) and issubclass(o, Plugin) and o.__module__ == '<run_path>':
                p = o()
                plugins[p.name] = p

    return plugins
