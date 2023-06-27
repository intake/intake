from hashlib import md5


def subclasses(cls):
    out = set()
    for cl in cls.__subclasses__():
        out.add(cl)
        out |= subclasses(cl)
    return out


class Tokenizable:
    _tok = None

    @property
    def token(self):
        # TODO: this effectively says that mutation, if allowed, does not change token
        if self._tok is None:
            # TODO: walk dict and use tokens of instances of Tokenizable therein?
            dic = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
            self._tok = md5(f"{self.__class__}{dic}".encode()).hexdigest()[:16]
        return self._tok

    def __hash__(self):
        """Hash depends on class name and all non-_* attributes"""
        return int(self.token, 16)
