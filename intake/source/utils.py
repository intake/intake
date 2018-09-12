from distutils.version import LooseVersion
try:
    import dask
    DASK_VERSION = LooseVersion(dask.__version__)
except:
    DASK_VERSION = None


def _get_parts_of_format_string(resolved_string, literal_texts, format_specs):
    """
    Inner function of reverse_format, returns the resolved value for each
    field in pattern.
    """
    _text = resolved_string
    bits = []

    for i, literal_text in enumerate(literal_texts):
        if literal_text != '':
            if literal_text not in _text:
                raise ValueError(("Resolved string must match pattern. "
                                  "'{}' not found.".format(literal_text)))
            bit, _text = _text.split(literal_text, 1)
            if bit:
                bits.append(bit)
        elif i == 0:
            continue
        else:
            format_spec = format_specs[i-1]
            if not format_spec:
                raise ValueError(('Format specifier must be set if '
                                  'no separator between fields.'))
            if format_spec[-1].isalpha():
                format_spec = format_spec[:-1]
            if not format_spec.isdigit():
                raise ValueError('Format specifier must have a set width')
            bits.append(_text[0:int(format_spec)])
            _text = _text[int(format_spec):]
    if _text:
        bits.append(_text)
    if len(bits) > len([fs for fs in format_specs if fs is not None]):
        bits = bits[1:]
    return bits


def reverse_format(format_string, resolved_string):
    """
    Reverse the string method format.

    Given format_string and resolved_string, find arguments that would
    give `format_string.format(**arguments) == resolved_string`

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
        `format_string.(**args) == resolved_string`

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
    """
    from string import Formatter
    from datetime import datetime

    fmt = Formatter()
    args = {}

    # split the string into bits
    literal_texts, field_names, format_specs, conversions = zip(*fmt.parse(format_string))
    if not any(field_names):
        return {}

    for i, conversion in enumerate(conversions):
        if conversion:
            raise ValueError(('Conversion not allowed. Found on {}.'
                              .format(field_names[i])))

    # get a list of the parts that matter
    bits = _get_parts_of_format_string(resolved_string, literal_texts, format_specs)

    for i, (field_name, format_spec) in enumerate(zip(field_names, format_specs)):
        if field_name is not None:
            try:
                if format_spec.startswith('%'):
                    args[field_name] = datetime.strptime(bits[i], format_spec)
                elif format_spec[-1] in list('bcdoxX'):
                    args[field_name] = int(bits[i])
                elif format_spec[-1] in list('eEfFgGn'):
                    args[field_name] = float(bits[i])
                elif format_spec[-1] == '%':
                    args[field_name] = float(bits[i][:-1])/100
                else:
                    args[field_name] = fmt.format_field(bits[i], format_spec)
            except:
                args[field_name] = bits[i]

    return args

def path_to_glob(path):
    """
    Convert pattern style paths to glob style paths

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

    # Get just the real bits of the urlpath
    literal_texts = [i[0] for i in fmt.parse(path)]

    # Only use a star for first empty string in literal_texts
    index_of_empty = [i for i, lt in enumerate(literal_texts) if lt == '' and i != 0]
    glob = '*'.join([literal_texts[i] for i in range(len(literal_texts)) if i not in index_of_empty])

    return glob


def path_to_pattern(path, metadata=None):
    """
    Remove source information from path when using chaching

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
    pattern = path
    if metadata:
        cache = metadata.get('cache')
        if cache:
            regex = next(c.get('regex') for c in cache if c.get('argkey') == 'urlpath')
            pattern = pattern.split(regex)[-1]
    return pattern


def unique_string():
    from string import ascii_letters, digits
    from random import choice

    return ''.join([choice(ascii_letters + digits) for n in range(8)])
