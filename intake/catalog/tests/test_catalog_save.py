"""
Test saving catalogs.
"""
import os

import intake
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry


def test_catalog_description(tmpdir):
    """Make sure the description comes through the save."""

    tmpdir = str(tmpdir)
    cat_path = os.path.join(tmpdir, "desc_test.yaml")

    cat1 = Catalog.from_dict(
        {
            "name": LocalCatalogEntry(
                "name",
                description="description",
                driver=intake.catalog.local.YAMLFileCatalog,
            )
        },
        name="overall catalog name",
        description="overall catalog description",
    )

    cat1.save(cat_path)

    cat2 = intake.open_catalog(cat_path)

    assert cat2.description is not None
