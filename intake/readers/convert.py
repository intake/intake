from intake.readers import readers

_converted = {}


def outtypes():
    """Available types we can convert *to*"""
    return set(_[1] for _ in _converted)


def can_provide(outtype: str):
    """What input types can make the given output type"""
    return set(intype for intype, out in _converted if out == outtype)


def register_converter(intype: str, outtype: str, clobber=True):
    """Add a convert function to the list of known conversions"""
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


@register_converter("ray.data:Dataset", "pandas:DataFrame")
def ray_to_pandas(x, **kw):
    return x.to_pandas(**kw)


@register_converter("ray.data:Dataset", "dask.dataframe:DataFrame")
def ray_to_daskdf(x, **kw):
    return x.to_dask(**kw)


def converts_to(data):
    """What things can data convert to"""
    out = set()
    package = type(data).__module__.split(".", 1)[0]
    for intype, outt in _converted:
        if intype.split(".", 1)[0] != package:
            continue
        thing = readers.import_name(intype)
        if isinstance(data, thing):
            out.add(outt)
    return out


def convert_func(data, outtype: str):
    """Get conversion function"""
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


def convert(data, outtype: str, **kwargs):
    """Convert this data to given type"""
    func = convert_func(data, outtype)
    return func(data, **kwargs)
