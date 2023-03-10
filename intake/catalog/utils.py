# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

import ast
import functools
import itertools
import os
import re
import shlex
import subprocess
import sys

from jinja2 import Environment, Undefined, meta


def flatten(iterable):
    """Flatten an arbitrarily deep list"""
    # likely not used
    iterable = iter(iterable)
    while True:
        try:
            item = next(iterable)
        except StopIteration:
            break

        if isinstance(item, str):
            yield item
            continue

        try:
            data = iter(item)
            iterable = itertools.chain(data, iterable)
        except Exception:
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


def _j_getenv(x, default=""):
    if isinstance(x, Undefined):
        x = x._undefined_name
    if isinstance(default, Undefined):
        default = default._undefined_name
    return os.getenv(x, default)


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
        return {k: _expand(v, context, all_vars, client, getenv, getshell) for k, v in p.items()}
    elif isinstance(p, (list, tuple, set)):
        return type(p)(_expand(v, context, all_vars, client, getenv, getshell) for v in p)
    elif isinstance(p, str):
        jinja = Environment()
        if getenv and not client:
            jinja.globals["env"] = _j_getenv
        else:
            jinja.globals["env"] = lambda x: _j_passthrough(x, funcname="env")
        if getenv and client:
            jinja.globals["client_env"] = _j_getenv
        else:
            jinja.globals["client_env"] = lambda x: _j_passthrough(x, funcname="client_env")
        if getshell and not client:
            jinja.globals["shell"] = _j_getshell
        else:
            jinja.globals["shell"] = lambda x: _j_passthrough(x, funcname="shell")
        if getshell and client:
            jinja.globals["client_shell"] = _j_getshell
        else:
            jinja.globals["client_shell"] = lambda x: _j_passthrough(x, funcname="client_shell")
        ast = jinja.parse(p)
        all_vars -= meta.find_undeclared_variables(ast)
        return jinja.from_string(p).render(context)
    else:
        # no expansion
        return p


def expand_templates(pars, context, return_left=False, client=False, getenv=True, getshell=True):
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
    r = re.match(r"env\((.*),?(.*)\)", default)
    if r and not client and getenv:
        gs = r.groups()
        default = os.environ.get(gs[0], gs[1] if len(gs) > 1 else "")
    r = re.match(r"client_env\((.*)\)", default)
    if r and client and getenv:
        default = os.environ.get(r.groups()[0], "")
    r = re.match(r"shell\((.*)\)", default)
    if r and not client and getshell:
        try:
            cmd = shlex.split(r.groups()[0])
            default = subprocess.check_output(cmd).rstrip().decode("utf8")
        except (subprocess.CalledProcessError, OSError):
            default = ""
    r = re.match(r"client_shell\((.*)\)", default)
    if r and client and getshell:
        try:
            cmd = shlex.split(r.groups()[0])
            default = subprocess.check_output(cmd).rstrip().decode("utf8")
        except (subprocess.CalledProcessError, OSError):
            default = ""
    return default


def merge_pars(params, user_inputs, spec_pars, client=False, getenv=True, getshell=True):
    """Produce open arguments by merging various inputs

    This function is called in the context of a catalog entry, when finalising
    the arguments for instantiating the corresponding data source.

    The three sets of inputs to be considered are:
    - the arguments section of the original spec (params)
    - UserParameters associated with the entry (spec_pars)
    - explicit arguments provided at instantiation time, like entry(arg=value)
      (user_inputs)

    Both spec_pars and user_inputs can be considered as template variables and
    used in expanding string values in params.

    The default value of a spec_par, if given, may have embedded env and shell
    functions, which will be evaluated before use, if the default is used and
    the corresponding getenv/getsgell are set. Similarly, string value params
    will also have access to these functions within jinja template groups,
    as well as full jinja processing.

    Where a key exists in both the spec_pars and the user_inputs, the
    user_input wins. Where user_inputs contains keys not seen elsewhere, they
    are regarded as extra kwargs to pass to the data source.

    Where spec pars have the same name as keys in params, their type, max/min
    and allowed fields are used to validate the final values of the
    corresponding arguments.

    Parameters
    ----------
    params : dict
        From the entry's original spec
    user_inputs : dict
        Provided by the user/calling function
    spec_pars : list of UserParameters
        Default and validation instances
    client : bool
        Whether this is all running on a client to a remote server - sets
        which of the env/shell functions are in operation.
    getenv : bool
        Whether to allow pulling environment variables. If False, the
        template blocks will pass through unevaluated
    getshell : bool
        Whether or not to allow executing of shell commands. If False, the
        template blocks will pass through unevaluated

    Returns
    -------
    Final parameter dict
    """
    context = params.copy()
    for par in spec_pars:
        val = user_inputs.get(par.name, par.default)
        if val is not None:
            if isinstance(val, str):
                val = expand_defaults(val, getenv=getenv, getshell=getshell, client=client)
            context[par.name] = par.validate(val)
    context.update({k: v for k, v in user_inputs.items() if k not in context})
    out, left = expand_templates(params, context, True, client, getenv, getshell)
    context = {k: v for k, v in context.items() if k in left}
    for par in spec_pars:
        if par.name in context:
            # coerces to type
            context[par.name] = par.validate(context[par.name])
            left.remove(par.name)

    params.update(out)
    user_inputs = expand_templates(user_inputs, context, False, client, getenv, getshell)
    params.update({k: v for k, v in user_inputs.items() if k in left})
    params.pop("CATALOG_DIR", None)
    for k, v in params.copy().items():
        # final validation/coersion
        for sp in [p for p in spec_pars if p.name == k]:
            params[k] = sp.validate(params[k])

    return params


def coerce_datetime(v=None):
    import pandas

    if not v:
        v = 0

    try:
        iter(v)
    except TypeError:  # not iterable
        pass
    else:
        if "__datetime__" in v:
            v = v["as_str"]

    return pandas.to_datetime(v)


def with_str_parse(value, rule):
    import ast

    if isinstance(value, str) and rule not in [str, coerce_datetime]:
        try:
            value = ast.literal_eval(value)
        except (ValueError, TypeError, RuntimeError):
            pass

    return rule(value)


COERCION_RULES = {
    "bool": bool,
    "datetime": coerce_datetime,
    "dict": dict,
    "float": float,
    "tuple": tuple,
    "mlist": list,
    "int": int,
    "list": list,
    "str": str,
    "unicode": str,
    "other": lambda x: x,
}


def coerce(dtype, value):
    """
    Convert a value to a specific type.

    If the value is already the given type, then the original value is
    returned. If the value is None, then the default value given by the
    type constructor is returned. Otherwise, the type constructor converts
    and returns the value.
    """
    if "[" in dtype:
        dtype, inner = dtype.split("[")
        inner = inner.rstrip("]")
    else:
        inner = None
    if dtype is None:
        return value
    if type(value).__name__ == dtype:
        return value
    if dtype == "mlist":
        if isinstance(value, (tuple, set, dict)):
            return list(value)
        if isinstance(value, str):
            try:
                value = ast.literal_eval(value)
                return list(value)
            except ValueError as e:
                raise ValueError("Failed to coerce string to list") from e
        return value
    op = COERCION_RULES[dtype]
    out = op() if value is None else with_str_parse(value, op)
    if isinstance(out, dict) and inner is not None:
        # TODO: recurse into coerce here, to allow list[list[str]] and such?
        out = {k: COERCION_RULES[inner](v) for k, v in out.items()}
    if isinstance(out, (tuple, list, set)) and inner is not None:
        out = op(COERCION_RULES[inner](v) for v in out)
    return out


class RemoteCatalogError(Exception):
    pass


def _has_catalog_dir(args):
    """Check is any value in args dict needs CATALOG_DIR variable"""
    env = Environment()
    for k, arg in args.items():
        parsed_content = env.parse(arg)
        vars = meta.find_undeclared_variables(parsed_content)
        if "CATALOG_DIR" in vars:
            return True
    return False
