#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from contextlib import contextmanager
import yaml


def make_path_posix(path):
    """ Make path generic """
    if '://' in path:
        return path
    return path.replace('\\', '/').replace('//', '/')


def no_duplicates_constructor(loader, node, deep=False):
    """Check for duplicate keys while loading YAML

    https://gist.github.com/pypt/94d747fe5180851196eb
    """

    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping:
            from intake.catalog.exceptions import DuplicateKeyError

            raise DuplicateKeyError("while constructing a mapping",
                                    node.start_mark,
                                    "found duplicate key (%s)" % key,
                                    key_node.start_mark)
        mapping[key] = value

    return loader.construct_mapping(node, deep)


@contextmanager
def no_duplicate_yaml():
    yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                         no_duplicates_constructor)
    try:
        yield
    finally:
        yaml.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            yaml.constructor.SafeConstructor.construct_yaml_map
        )


def yaml_load(stream):
    with no_duplicate_yaml():
        return yaml.load(stream)
