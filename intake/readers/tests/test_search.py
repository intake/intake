from intake.readers.entry import Catalog, ReaderDescription
from intake.readers.readers import BaseReader
from intake.readers.search import Importable, Text


class NotImportable(BaseReader):
    # TODO: this will show up in the class hierarchy throughout testing - make a temporary mock?
    #  see https://stackoverflow.com/a/52428851/3821154
    imports = {"unknown_package"}


def test_1():
    cat = Catalog()
    cat["en1"] = ReaderDescription("intake.readers.readers:BaseReader", kwargs={"allow_me": True})
    cat["en2"] = ReaderDescription("intake.readers.readers:BaseReader", kwargs={"nope": True})
    cat["en3"] = ReaderDescription(
        "intake.readers.tests.test_search:NotImportable", kwargs={"allow_me": True}
    )

    # simple text
    cat2 = cat.search("allow")
    assert "en1" in cat2
    assert "en2" not in cat2
    assert "en3" in cat2
    assert cat2["en3"] == cat["en3"]

    # single term
    cat2 = cat.search(Importable())
    assert "en1" in cat2
    assert "en2" in cat2
    assert "en3" not in cat2

    # expression
    cat2 = cat.search(Importable() & Text("allow"))
    assert "en1" in cat2
    assert "en2" not in cat2
    assert "en3" not in cat2
