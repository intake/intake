import os
import yaml
import jinja2

from ..source import registry


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

        self._entries = {
            key: parse_catalog_entry(value, catalog_dir=self._catalog_dir)
            for key, value in catalog_yaml.items()
        }

    def list(self):
        return list(self._entries.keys())

    def describe(self, entry_name):
        return self._entries[entry_name].describe() 

    def get(self, entry_name, **user_parameters):
        return self._entries[entry_name].get(**user_parameters)


class LocalCatalogEntry:
    def __init__(self, description, plugin, open_args, user_parameters, catalog_dir):
        self._description = description
        self._plugin = plugin
        self._open_args = open_args
        self._user_parameters = user_parameters
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

        # FIXME: Who cleans this up??
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


def parse_catalog_entry(entry, catalog_dir):
    description = entry.get('description', '')
    plugin = registry[entry['driver']]
    open_args = entry['args']
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
        catalog_dir=catalog_dir)
