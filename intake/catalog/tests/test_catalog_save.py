"""
Test saving catalogs.
"""
import os
import intake
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry


def test_catalog_description():
    """Make sure the description comes through the save."""

    NAME = 'desc_test.yaml'

    cat1 = Catalog.from_dict({
                'name': LocalCatalogEntry('name',
                                          description='description',
                                          driver=intake.catalog.local.YAMLFileCatalog,
                                            )
    },
                name='overall catalog name',
                description='overall catalog description'

    )

    cat1.save(NAME)

    cat2 = intake.open_catalog(NAME)

    assert cat2.description is not None

    os.remove(NAME)
