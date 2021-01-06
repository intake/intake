import os
import intake

catfile = os.path.join(os.path.dirname(__file__), "..", "..",
                       "catalog", "tests", "catalog_alias.yml")


def test_columns():
    cat = intake.open_catalog(catfile)
    df1 = cat.test_cache.read()
    df2 = cat.derive_cols.read()
    assert df1[["state", "slug"]].equals(df2)
