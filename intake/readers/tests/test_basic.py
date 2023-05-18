import os

from intake.readers import datatypes, readers

here = os.path.dirname(__file__)
testdir = os.path.abspath(os.path.join(here, "..", "..", "catalog/tests"))


def test1():
    data = datatypes.CSV(url=f"{testdir}/entry1_1.csv")
    reader = readers.PandasCSV(data)
    out = reader.read()
    assert list(out.columns) == ["name", "score", "rank"]


def test_recomment_filetype():
    assert datatypes.recommend(url="myfile.parq") == {datatypes.Parquet}
    assert datatypes.recommend(mime="text/yaml") == {datatypes.YAMLFile, datatypes.CatalogFile, datatypes.Text}


def test_recommend_reader():
    pp = datatypes.Parquet()
    assert readers.recommend(pp) == {readers.PandasParquet}
