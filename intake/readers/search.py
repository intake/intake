from intake.readers.entry import ReaderDescription

# some inspiration https://blueskyproject.io/tiled/reference/queries.html


class SearchBase:
    """Prototype for a single term in a search expression

    The method `filter()` is meant to be overridden in subclasses.
    """

    def filter(self, entry: ReaderDescription) -> bool:
        # should not raise: an exception counts as False
        return True

    def __or__(self, other):
        return Or(self, other)

    def __and__(self, other):
        return And(self, other)

    def __inv__(self):
        return Not(self)


class Or(SearchBase):
    def __init__(self, first: SearchBase, second: SearchBase):
        self.first = first
        self.second = second

    def filter(self, entry: ReaderDescription) -> bool:
        return self.first.filter(entry) or self.second.filter(entry)


class And(SearchBase):
    def __init__(self, first: SearchBase, second: SearchBase):
        self.first = first
        self.second = second

    def filter(self, entry: ReaderDescription) -> bool:
        return self.first.filter(entry) and self.second.filter(entry)


class Not(SearchBase):
    def __init__(self, first: SearchBase):
        self.first = first

    def filter(self, entry: ReaderDescription) -> bool:
        return not self.first.filter(entry)


class Any(SearchBase):
    def __init__(self, *terms: tuple[SearchBase, ...]):
        self.terms = terms

    def filter(self, entry: ReaderDescription) -> bool:
        return any(t.filter(entry) for t in self.terms)


class All(SearchBase):
    def __init__(self, *terms: tuple[SearchBase, ...]):
        self.terms = terms

    def filter(self, entry: ReaderDescription) -> bool:
        return all(t.filter(entry) for t in self.terms)


class Text(SearchBase):
    def __init__(self, text: str):
        self.text = text

    def filter(self, entry: ReaderDescription) -> bool:
        return self.text in str(entry)


class Importable(SearchBase):
    def filter(self, entry: ReaderDescription) -> bool:
        return entry.check_imports()


class EnvironmentSatisfied(SearchBase):
    def filter(self, entry: ReaderDescription, output=False) -> bool:
        # TODO: this is quite slow, should cache?
        import fsspec
        import os

        env = entry.metadata.get("environment")
        if not env:
            # no env restrictions means a pass
            return True
        import subprocess
        import tempfile
        import shlex

        fn = tempfile.mktemp(suffix=".yaml")
        try:
            if "dependencies:" not in env:
                with fsspec.open(env, "rt") as f:
                    env = f.read()
            with open(fn, "wt") as f:
                f.write(env)

            cmd = shlex.split(f"conda compare {fn}")
            kw = {} if output else dict(stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            out = subprocess.check_call(cmd, **kw)
            return out == 0
        except Exception:
            return False
        finally:
            if os.path.exists(fn):
                os.remove(fn)
