import os

import pytest

import intake.readers.datatypes

here = os.path.dirname(os.path.abspath(__file__))
cat_url = os.path.join(here, "stac_data", "1.0.0", "catalog", "catalog.json")
pytest.importorskip("pystac")


def test_1():
    data = intake.readers.datatypes.JSONFile(cat_url)
    cat = data.to_reader(reader="StacCatalog").read()
    assert "test" in cat
    cat2 = cat.test.read()
    assert isinstance(cat2, intake.readers.entry.Catalog)
    assert cat2.metadata["description"] == "child catalog"
