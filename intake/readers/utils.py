def subclasses(cls):
    out = set()
    for cl in cls.__subclasses__():
        out.add(cl)
        out |= subclasses(cl)
    return out
