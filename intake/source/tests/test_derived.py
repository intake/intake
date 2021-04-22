import os
import intake

catfile = os.path.join(os.path.dirname(__file__), "..", "..",
                       "catalog", "tests", "catalog_alias.yml")


def test_columns():
    cat = intake.open_catalog(catfile)
    df1 = cat.input_data.read()
    df2 = cat.derive_cols.read()
    assert df1[["state", "slug"]].equals(df2)


def _pick_columns(df, columns):
    return df[columns]


def test_df_transform():
    cat = intake.open_catalog(catfile)
    df1 = cat.input_data.read()
    df2 = cat.derive_cols_func.read()
    assert df1[["state", "slug"]].equals(df2)


def test_barebones():
    cat = intake.open_catalog(catfile)
    df1 = cat.input_data.read()
    cat = intake.open_catalog(catfile)
    assert cat.barebones.read() == len(df1)


def test_other_cat():
    cat = intake.open_catalog(catfile)
    df1 = cat.other_cat.read()
    assert df1.columns.tolist() == ["name", "score"]
