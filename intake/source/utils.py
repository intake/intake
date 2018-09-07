def reverse_format(format_string, resolved_string):
    from string import Formatter
    from datetime import datetime

    fmt = Formatter()
    args = {}
    _text = resolved_string

    # split the string into bits
    literal_texts, field_names, format_specs, conversions = zip(*fmt.parse(format_string))
    if not any(field_names):
        return {}

    for i, conversion in enumerate(conversions):
        if conversion:
            raise ValueError('Conversion not allowed. Found on {}.'.format(field_names[i]))

    # get a list of the parts that matter
    bits = []
    for i, literal_text in enumerate(literal_texts):
        if literal_text != '':
            bit, _text = _text.split(literal_text, 1)
            if bit:
                bits.append(bit)
        elif i == 0:
            continue
        else:
            fs = format_specs[i-1]
            if not fs:
                raise ValueError('Format specifier must be set if no separator between fields.')
            if fs[-1].isalpha():
                fs = fs[:-1]
            if not fs.isdigit():
                raise ValueError('Format specifier must have a set width')
            bits.append(_text[0:int(fs)])
            _text = _text[int(fs):]
    if _text:
        bits.append(_text)

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
