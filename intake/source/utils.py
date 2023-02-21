# -----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

from packaging.version import Version

try:
    import dask

    DASK_VERSION = Version(dask.__version__)
except ImportError:
    DASK_VERSION = None
from ..utils import make_path_posix


def _validate_format_spec(format_spec):
    if not format_spec:
        raise ValueError(("Format specifier must be set if " "no separator between fields."))
    if format_spec[-1].isalpha():
        format_spec = format_spec[:-1]
    if not format_spec.isdigit():
        raise ValueError("Format specifier must have a set width")
    return int(format_spec)


def _get_parts_of_format_string(resolved_string, literal_texts, format_specs):
    """
    Inner function of reverse_format, returns the resolved value for each
    field in pattern.
    """
    _text = resolved_string
    bits = []

    if literal_texts[-1] != "" and _text.endswith(literal_texts[-1]):
        _text = _text[: -len(literal_texts[-1])]
        literal_texts = literal_texts[:-1]
        format_specs = format_specs[:-1]

    for i, literal_text in enumerate(literal_texts):
        if literal_text != "":
            if literal_text not in _text:
                raise ValueError(("Resolved string must match pattern. " "'{}' not found.".format(literal_text)))
            bit, _text = _text.split(literal_text, 1)
            if bit:
                bits.append(bit)
        elif i == 0:
            continue
        else:
            try:
                format_spec = _validate_format_spec(format_specs[i - 1])
                bits.append(_text[0:format_spec])
                _text = _text[format_spec:]
            except Exception:
                if i == len(format_specs) - 1:
                    format_spec = _validate_format_spec(format_specs[i])
                    bits.append(_text[:-format_spec])
                    bits.append(_text[-format_spec:])
                    _text = []
                else:
                    _validate_format_spec(format_specs[i - 1])
    if _text:
        bits.append(_text)
    if len(bits) > len([fs for fs in format_specs if fs is not None]):
        bits = bits[1:]
    return bits


def reverse_formats(format_string, resolved_strings):
    """
    Reverse the string method format for a list of strings.

    Given format_string and resolved_strings, for each resolved string
    find arguments that would give
    ``format_string.format(**arguments) == resolved_string``.

    Each item in the output corresponds to a new column with the key setting
    the name and the values representing a mapping from list of resolved_strings
    to the related value.

    Parameters
    ----------
    format_strings : str
        Format template string as used with str.format method
    resolved_strings : list
        List of strings with same pattern as format_string but with fields
        filled out.

    Returns
    -------
    args : dict
        Dict of the form ``{field: [value_0, ..., value_n], ...}`` where values are in
        the same order as resolved_strings, so:
        ``format_sting.format(**{f: v[0] for f, v in args.items()}) == resolved_strings[0]``

    Examples
    --------

    >>> paths = ['data_2014_01_03.csv', 'data_2014_02_03.csv', 'data_2015_12_03.csv']
    >>> reverse_formats('data_{year}_{month}_{day}.csv', paths)
    {'year':  ['2014', '2014', '2015'],
     'month': ['01', '02', '12'],
     'day':   ['03', '03', '03']}
    >>> reverse_formats('data_{year:d}_{month:d}_{day:d}.csv', paths)
    {'year': [2014, 2014, 2015], 'month': [1, 2, 12], 'day': [3, 3, 3]}
    >>> reverse_formats('data_{date:%Y_%m_%d}.csv', paths)
    {'date': [datetime.datetime(2014, 1, 3, 0, 0),
              datetime.datetime(2014, 2, 3, 0, 0),
              datetime.datetime(2015, 12, 3, 0, 0)]}
    >>> reverse_formats('{state:2}{zip:5}', ['PA19104', 'PA19143', 'MA02534'])
    {'state': ['PA', 'PA', 'MA'], 'zip': ['19104', '19143', '02534']}

    See also
    --------
    str.format : method that this reverses
    reverse_format : method for reversing just one string using a pattern
    """
    from string import Formatter

    fmt = Formatter()

    # get the fields from the format_string
    field_names = [i[1] for i in fmt.parse(format_string) if i[1]]

    # itialize the args dict with an empty dict for each field
    args = {field_name: [] for field_name in field_names}
    for resolved_string in resolved_strings:
        for field, value in reverse_format(format_string, resolved_string).items():
            args[field].append(value)

    return args


def reverse_format(format_string, resolved_string):
    """
    Reverse the string method format.

    Given format_string and resolved_string, find arguments that would
    give ``format_string.format(**arguments) == resolved_string``

    Parameters
    ----------
    format_string : str
        Format template string as used with str.format method
    resolved_string : str
        String with same pattern as format_string but with fields
        filled out.

    Returns
    -------
    args : dict
        Dict of the form {field_name: value} such that
        ``format_string.(**args) == resolved_string``

    Examples
    --------

    >>> reverse_format('data_{year}_{month}_{day}.csv', 'data_2014_01_03.csv')
    {'year': '2014', 'month': '01', 'day': '03'}
    >>> reverse_format('data_{year:d}_{month:d}_{day:d}.csv', 'data_2014_01_03.csv')
    {'year': 2014, 'month': 1, 'day': 3}
    >>> reverse_format('data_{date:%Y_%m_%d}.csv', 'data_2016_10_01.csv')
    {'date': datetime.datetime(2016, 10, 1, 0, 0)}
    >>> reverse_format('{state:2}{zip:5}', 'PA19104')
    {'state': 'PA', 'zip': '19104'}

    See also
    --------
    str.format : method that this reverses
    reverse_formats : method for reversing a list of strings using one pattern
    """
    from datetime import datetime
    from string import Formatter

    fmt = Formatter()
    args = {}

    # ensure that format_string is in posix format
    format_string = make_path_posix(format_string)

    # split the string into bits
    literal_texts, field_names, format_specs, conversions = zip(*fmt.parse(format_string))
    if not any(field_names):
        return {}

    for i, conversion in enumerate(conversions):
        if conversion:
            raise ValueError(("Conversion not allowed. Found on {}.".format(field_names[i])))

    # ensure that resolved string is in posix format
    resolved_string = make_path_posix(resolved_string)

    # get a list of the parts that matter
    bits = _get_parts_of_format_string(resolved_string, literal_texts, format_specs)

    for i, (field_name, format_spec) in enumerate(zip(field_names, format_specs)):
        if field_name:
            try:
                if format_spec.startswith("%"):
                    args[field_name] = datetime.strptime(bits[i], format_spec)
                elif format_spec[-1] in list("bcdoxX"):
                    args[field_name] = int(bits[i])
                elif format_spec[-1] in list("eEfFgGn"):
                    args[field_name] = float(bits[i])
                elif format_spec[-1] == "%":
                    args[field_name] = float(bits[i][:-1]) / 100
                else:
                    args[field_name] = fmt.format_field(bits[i], format_spec)
            except Exception:
                args[field_name] = bits[i]

    return args


def path_to_glob(path):
    """
    Convert pattern style paths to glob style paths

    Returns path if path is not str

    Parameters
    ----------
    path : str
        Path to data optionally containing format_strings

    Returns
    -------
    glob : str
        Path with any format strings replaced with *

    Examples
    --------

    >>> path_to_glob('{year}/{month}/{day}.csv')
    '*/*/*.csv'
    >>> path_to_glob('data/{year:4}{month:02}{day:02}.csv')
    'data/*.csv'
    >>> path_to_glob('data/*.csv')
    'data/*.csv'
    """
    from string import Formatter

    fmt = Formatter()

    if not isinstance(path, str):
        return path

    # calculate glob expression
    glob = ""
    prev_field_name = None
    for literal_text, field_name, _, _ in fmt.parse(path):
        glob += literal_text
        # condition to avoid repeated * on adjacent fields
        if field_name and (literal_text or prev_field_name is None):
            glob += "*"
        prev_field_name = field_name
    return glob


def path_to_pattern(path, metadata=None):
    """
    Remove source information from path when using chaching

    Returns None if path is not str

    Parameters
    ----------
    path : str
        Path to data optionally containing format_strings
    metadata : dict, optional
        Extra arguments to the class, contains any cache information

    Returns
    -------
    pattern : str
        Pattern style path stripped of everything to the left of cache regex.
    """
    from fsspec.core import strip_protocol

    if not isinstance(path, str):
        return

    pattern = strip_protocol(path)
    if metadata:
        cache = metadata.get("cache")
        if cache:
            regex = next(c.get("regex") for c in cache if c.get("argkey") == "urlpath")
            pattern = pattern.split(regex)[-1]
    return pattern


def unique_string():
    from random import choice
    from string import ascii_letters, digits

    return "".join([choice(ascii_letters + digits) for n in range(8)])
