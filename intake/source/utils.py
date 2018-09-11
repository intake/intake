def _get_parts_of_format_string(resolved_string, literal_texts, format_specs):
    _text = resolved_string
    bits = []

    for i, literal_text in enumerate(literal_texts):
        if literal_text != '':
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

def pattern_to_glob(format_string):
    from string import Formatter

    fmt = Formatter()

    # Get just the real bits of the format_string
    literal_texts = [i[0] for i in fmt.parse(format_string)]

    # Only use a star for first empty string in literal_texts
    index_of_empty = [i for i, lt in enumerate(literal_texts) if lt == '' and i != 0]
    glob = '*'.join([literal_texts[i] for i in range(len(literal_texts)) if i not in index_of_empty])

    return glob


def unique_string():
    from string import ascii_letters, digits
    from random import choice

    return ''.join([choice(ascii_letters + digits) for n in range(8)])
