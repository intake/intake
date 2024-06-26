import os

from intake.readers import datatypes, readers, entry

here = os.path.dirname(__file__)
testdir = os.path.abspath(os.path.join(here, "..", "..", "catalog/tests"))


def test1():
    data = datatypes.CSV(url=f"{testdir}/entry1_1.csv")
    reader = readers.PandasCSV(data)
    assert reader.doc()
    out = reader.read()
    assert list(out.columns) == ["name", "score", "rank"]


def test_recommend_filetype():
    assert datatypes.Parquet in datatypes.recommend(url="myfile.parq")
    assert datatypes.Parquet in datatypes.recommend(head=b"PAR1")
    assert all(
        p in datatypes.recommend(mime="text/yaml", head=False)
        for p in {datatypes.YAMLFile, datatypes.CatalogFile, datatypes.Text}
    )


def test_recommend_reader():
    pp = datatypes.Parquet("")
    rec = readers.recommend(pp)
    assert all(p not in rec["importable"] for p in rec["not_importable"])
    assert all(p not in rec["not_importable"] for p in rec["importable"])
    assert all(
        p in rec["importable"] + rec["not_importable"]
        for p in {readers.PandasParquet, readers.AwkwardParquet, readers.DaskParquet}
    )
    pp = datatypes.CSV("")
    assert readers.PandasCSV in readers.recommend(pp)["importable"]


def test_data_metadata():
    cat = entry.Catalog()
    cat["d"] = datatypes.BaseData(metadata={"oi": "io"})
    cat.get_entity("d").metadata.update(blag=0)
    out = cat["d"]
    assert out.metadata == {"oi": "io", "blag": 0}
