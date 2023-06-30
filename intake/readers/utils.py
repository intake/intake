from hashlib import md5


def subclasses(cls):
    out = set()
    for cl in cls.__subclasses__():
        out.add(cl)
        out |= subclasses(cl)
    return out


def merge_dicts(*dicts):
    out = {}
    for dic in dicts:
        for k, v in dic.items():
            out[k] = v
    return out


class Tokenizable:
    _tok = None

    @property
    def token(self):
        # TODO: this effectively says that mutation, if allowed, does not change token
        #  implyng that only _ attributes are multable, such as _metadata
        if self._tok is None:
            # TODO: walk dict and use tokens of instances of Tokenizable therein?
            dic = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
            self._tok = md5(f"{self.qname()}|{dic}".encode()).hexdigest()[:16]
        return self._tok

    def __hash__(self):
        """Hash depends on class name and all non-_* attributes"""
        return int(self.token, 16)

    def __eq__(self, other):
        return self.token == other.token

    @classmethod
    def qname(cls):
        """module:class name of this class, makes str for import_name"""
        return f"{cls.__module__}:{cls.__name__}"
