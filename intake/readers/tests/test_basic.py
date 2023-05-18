import os

from intake.readers import datatypes, readers

here = os.path.dirname(__file__)
testdir = os.path.abspath(os.path.join(here, "..", "..", "catalog/tests"))


def test1():
    data = datatypes.CSV(url=f"{testdir}/entry1_1.csv")
    reader = readers.PandasCSV(data)
    assert reader.doc()
    out = reader.read()
    assert list(out.columns) == ["name", "score", "rank"]


def test_recommend_filetype():
    assert datatypes.recommend(url="myfile.parq") == {datatypes.Parquet}
    assert datatypes.recommend(head=b"PAR1") == {datatypes.Parquet}
    assert datatypes.recommend(mime="text/yaml") == {datatypes.YAMLFile, datatypes.CatalogFile, datatypes.Text}


def test_recommend_reader():
    pp = datatypes.Parquet()
    assert readers.recommend(pp) == {readers.PandasParquet, readers.AwkwardParquet, readers.DaskParquet, readers.DuckDB, readers.PandasDuck}
