import ruamel.yaml

from . import output, transform
from .convert import Pipeline
from .entry import Catalog, DataDescription, ReaderDescription
from .readers import BaseReader
from .user_parameters import BaseUserParameter, SimpleUserParameter

YAML = ruamel.yaml.YAML()
YAML.register_class(BaseUserParameter)
YAML.register_class(SimpleUserParameter)
YAML.register_class(DataDescription)
YAML.register_class(ReaderDescription)
YAML.register_class(Catalog)
