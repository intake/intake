"""
Test saving catalogs.
"""

import intake
from intake.catalog import Catalog
from intake.catalog.local import LocalCatalogEntry


def test_catalog_description():
    """Make sure the description comes through the save."""

    cat1 = Catalog.from_dict({
                'name': LocalCatalogEntry('name',
                                          description='description',
                                          driver=intake.catalog.local.YAMLFileCatalog,
                                            )
    },
                name='overall catalog name',
                description='overall catalog description'

    )

    cat1.save('desc_test.yaml')

    cat2 = intake.open_catalog('desc_test.yaml')

    assert cat2.description is not None
