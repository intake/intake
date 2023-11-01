from intake.readers.entry import ReaderDescription

# some inspiration https://blueskyproject.io/tiled/reference/queries.html


class SearchBase:
    """Prototype for a single term in a search expression

    The method `filter()` is meant to be overridden in subclasses.
    """

    def filter(self, entry: ReaderDescription) -> bool:
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
