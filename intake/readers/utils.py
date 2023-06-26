from hashlib import md5


def subclasses(cls):
    out = set()
    for cl in cls.__subclasses__():
        out.add(cl)
        out |= subclasses(cl)
    return out


class Tokenizable:
    @property
    def token(self):
        dic = {k: v for k, v in self.__dict__ if not k.startswith("_")}
        return md5(f"{self.__class__}{dic}".encode()).hexdigest()[:16]

    def __hash__(self):
        """Hash depends on class name and all non-_* attributes"""
        return int(self.token, 16)
