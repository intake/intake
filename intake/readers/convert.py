from intake.readers import readers

_converted = {}


def outtypes():
    """Available types we can convert *to*"""
    return set(_[1] for _ in _converted)


def can_provide(outtype):
    return set(intype for intype, out in _converted if out == outtype)


def register_converter(intype: str, outtype: str, clobber=True):
    if not clobber and (intype, outtype) in _converted:
        raise ValueError

    def f(func):
        _converted[(intype, outtype)] = func
        return func

    return f


@register_converter("duckdb:DuckDBPyRelation", "pandas:DataFrame")
def duck_to_pandas(x, **kw):
    return x.df(**kw)


@register_converter("dask.dataframe:DataFrame", "pandas:DataFrame")
def daskdf_to_pandas(x, **kw):
    return x.compute(**kw)


def convert_func(data, outtype):
    package = type(data).__module__.split(".", 1)[0]
    for intype, out in _converted:
        if out != outtype:
            continue
        if intype.split(".", 1)[0] != package:
            continue
        thing = readers.import_name(intype)
        if isinstance(data, thing):
            return _converted[(intype, out)]
    raise ValueError("Converter not found")


def convert(data, outtype, **kwargs):
    func = convert_func(data, outtype)
    return func(data, **kwargs)
