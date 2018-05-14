import functools
import itertools
from jinja2 import Environment, meta, Template
import os
import re
import shlex
import subprocess
import sys

import six


def flatten(iterable):
    """Flatten an arbitrarily deep list"""
    iterable = iter(iterable)
    while True:
        try:
            item = next(iterable)
        except StopIteration:
            break

        if isinstance(item, six.string_types):
            yield item
            continue

        try:
            data = iter(item)
            iterable = itertools.chain(data, iterable)
        except:
            yield item


def reload_on_change(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.changed:
            self.reload()
        return f(self, *args, **kwargs)

    return wrapper


def clamp(value, lower=0, upper=sys.maxsize):
    """Clamp float between given range"""
    return max(lower, min(upper, value))


def make_prefix_tree(flat_dict):
    """Convert a dictionary with keys of dotted strings into nested dicts with common prefixes.

    Ex: { 'abc.xyz': 1, 'abc.def': 2, 'www': 3} is converted to:
        { 'abc': {'xyz': 1, 'def': 2}, 'www': 3 }
    """
    tree = {}
    for key, value in flat_dict.items():
        subtree = tree
        parts = key.split('.')
        for part in parts[:-1]:
            if part not in subtree:
                subtree[part] = {}
            subtree = subtree[part]
        subtree[parts[-1]] = value

    return tree


def expand_templates(pars, context, return_left=False):
    """
    Render variables in context into the set of parameters with jinja2.

    For variables that are not strings, nothing happens.

    Parameters
    ----------
    pars: dict
        values are strings containing some jinja2 controls
    context: dict
        values to use while rendering
    return_left: bool
        whether to return the set of variables in context that were not used
        in rendering parameters

    Returns
    -------
    dict with the same keys as ``pars``, but updated values; optionally also
    return set of unused parameter names.
    """
    all_vars = set(context)
    out = {}
    for k, v in pars.items():
        ast = Environment().parse(v)
        all_vars -= meta.find_undeclared_variables(ast)
        if isinstance(v, six.string_types):
            out[k] = Template(v).render(context)
        else:
            out[k] = v
    if return_left:
        return out, all_vars
    return out


def expand_defaults(default, client=False, getenv=True, getshell=True):
    """Compile env, client_env, shell and client_shell commands

    Execution rules:
    - env() and shell() execute on server or client, if getenv and getshell
      are True, respectively
    - client_env() and client_shell() execute only if client is True and
      getenv/getshell are also True.

    If both getenv and getshell are False, this method does nothing.

    If the environment variable is missing or the shell command fails, the
    output is an empty string.
    """
    r = re.match(r'env\((.*)\)', default)
    if r and not client and getenv:
        default = os.environ.get(r.groups()[0], '')
    r = re.match(r'client_env\((.*)\)', default)
    if r and client and getenv:
        default = os.environ.get(r.groups()[0], '')
    r = re.match(r'shell\((.*)\)', default)
    if r and not client and getshell:
        try:
            cmd = shlex.split(r.groups()[0])
            default = subprocess.check_output(
                cmd).rstrip().decode('utf8')
        except (subprocess.CalledProcessError, OSError):
            default = ''
    r = re.match(r'client_shell\((.*)\)', default)
    if r and client and getshell:
        try:
            cmd = shlex.split(r.groups()[0])
            default = subprocess.check_output(
                cmd).rstrip().decode('utf8')
        except (subprocess.CalledProcessError, OSError):
            default = ''
    return default


def coerce_datetime(v=None):
    import pandas
    return pandas.to_datetime(v) if v else pandas.to_datetime(0)


COERCION_RULES = {
    'bool': bool,
    'datetime': coerce_datetime,
    'float': float,
    'int': int,
    'list': list,
    'str': str,
    'unicode': six.text_type
}


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
    op = COERCION_RULES[dtype]
    return op() if value is None else op(value)
