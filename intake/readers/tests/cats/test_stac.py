import os

import pytest

import intake.readers.datatypes

here = os.path.dirname(os.path.abspath(__file__))
cat_url = os.path.join(here, "stac_data", "1.0.0", "catalog", "catalog.json")
simple_item_url = os.path.join(here, "stac_data", "1.0.0", "collection", "simple-item.json")
pytest.importorskip("pystac")


def test_1():
    data = intake.readers.datatypes.STACJSON(cat_url)
    cat = data.to_reader(reader="StacCatalog").read()
    assert "test" in cat
    cat2 = cat.test.read()
    assert isinstance(cat2, intake.entry.Catalog)
    assert cat2.metadata["description"] == "child catalog"


def test_bands():
    data = intake.readers.datatypes.STACJSON(simple_item_url)
    list_of_bands = ["B02", "B03"]
    reader = data.to_reader_cls(reader="Bands")(data, list_of_bands).read()
    assert isinstance(reader.kwargs["data"], intake.readers.datatypes.TIFF)
    assert len(reader.kwargs["data"].url) == 2
