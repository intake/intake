#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

import functools
import itertools
from jinja2 import Environment, meta, Undefined
import os
import re
import shlex
import subprocess
import sys

import six


def flatten(iterable):
    """Flatten an arbitrarily deep list"""
    # likely not used
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
        self.reload()
        return f(self, *args, **kwargs)

    return wrapper


def clamp(value, lower=0, upper=sys.maxsize):
    """Clamp float between given range"""
    return max(lower, min(upper, value))


def _j_getenv(x):
    if isinstance(x, Undefined):
        x = x._undefined_name
    return os.getenv(x, '')


def _j_getshell(x):
    if isinstance(x, Undefined):
        x = x._undefined_name
    try:
        return subprocess.check_output(x).decode()
    except (IOError, OSError):
        return ""


def _j_passthrough(x, funcname):
    if isinstance(x, Undefined):
        x = x._undefined_name
    return "{{%s(%s)}}" % (funcname, x)


def _expand(p, context, all_vars, client, getenv, getshell):
    if isinstance(p, dict):
        return {k: _expand(v, context, all_vars, client, getenv, getshell)
                for k, v in p.items()}
    elif isinstance(p, (list, tuple, set)):
        return type(p)(_expand(v, context, all_vars, client, getenv, getshell)
                       for v in p)
    elif isinstance(p, six.string_types):
        jinja = Environment()
        if getenv and not client:
            jinja.globals['env'] = _j_getenv
        else:
            jinja.globals['env'] = lambda x: _j_passthrough(x, funcname='env')
        if getenv and client:
            jinja.globals['client_env'] = _j_getenv
        else:
            jinja.globals['client_env'] = lambda x: _j_passthrough(x, funcname='client_env')
        if getshell and not client:
            jinja.globals['shell'] = _j_getshell
        else:
            jinja.globals['shell'] = lambda x: _j_passthrough(x, funcname='shell')
        if getshell and client:
            jinja.globals['client_shell'] = _j_getshell
        else:
            jinja.globals['client_shell'] = lambda x: _j_passthrough(x, funcname='client_shell')
        ast = jinja.parse(p)
        all_vars -= meta.find_undeclared_variables(ast)
        return jinja.from_string(p).render(context)
    else:
        # no expansion
        return p


def expand_templates(pars, context, return_left=False, client=False,
                     getenv=True, getshell=True):
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
    out = _expand(pars, context, all_vars, client, getenv, getshell)
    if return_left:
        return out, all_vars
    return out


def expand_defaults(default, client=False, getenv=True, getshell=True):
    """Compile env, client_env, shell and client_shell commands

    Execution rules:
    - env() and shell() execute where the cat is loaded, if getenv and getshell
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


def merge_pars(params, user_inputs, spec_pars, client=False, getenv=True,
               getshell=True):
    context = params.copy()
    for par in spec_pars:
        val = user_inputs.get(par.name, par.default)
        if val is not None:
            if isinstance(val, six.string_types):
                val = expand_defaults(val, getenv=getenv, getshell=getshell,
                                      client=client)
            context[par.name] = par.validate(val)
    context.update({k: v for k, v in user_inputs.items() if k not in context})
    out, left = expand_templates(params, context, True, client, getenv,
                                 getshell)
    context = {k: v for k, v in context.items() if k in left}
    for par in spec_pars:
        if par.name in context:
            # coerces to type
            context[par.name] = par.validate(context[par.name])
            left.remove(par.name)

    params.update(out)
    user_inputs = expand_templates(user_inputs, context, False, client, getenv,
                                   getshell)
    params.update({k: v for k, v in user_inputs.items() if k in left})
    params.pop('CATALOG_DIR')
    for k, v in params.copy().items():
        # final validation/coersion
        for sp in [p for p in spec_pars if p.name == k]:
            params[k] = sp.validate(params[k])

    return params


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
    if dtype is None:
        return value
    if type(value).__name__ == dtype:
        return value
    op = COERCION_RULES[dtype]
    return op() if value is None else op(value)


class RemoteCatalogError(Exception):
    pass
