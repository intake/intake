import fsspec
import pytest

import intake.readers

pd = pytest.importorskip("pandas")
bindata = b"apple,beet,carrot\n" + b"a,1,0.1\nb,2,0.2\nc,3,0.3\n" * 100


@pytest.fixture()
def dataframe_file():
    m = fsspec.filesystem("memory")
    m.pipe("/data", bindata)
    return "memory://data"


@pytest.fixture()
def df(dataframe_file):
    return pd.read_csv(dataframe_file)


def test_pipelines_in_catalogs(dataframe_file, df):
    data = intake.readers.datatypes.CSV(url=dataframe_file)
    reader = intake.readers.readers.PandasCSV(data)
    reader2 = reader[["apple", "beet"]].set_index(keys="beet")
    cat = intake.readers.entry.Catalog()
    cat["mydata"] = reader2

    assert cat.mydata.read().equals(df[["apple", "beet"]].set_index(keys="beet"))
    assert cat.mydata.discover().equals(df[["apple", "beet"]].set_index("beet")[:10])

    cat["eq"] = reader.equals(other=reader)
    assert reader.equals(other=reader).read() is True
    assert cat.eq.read() is True


def test_parameters(dataframe_file, monkeypatch):
    data = intake.readers.datatypes.CSV(url=dataframe_file)
    reader = intake.readers.readers.PandasCSV(data)
    reader2 = reader[["apple", "beet"]].set_index(keys="beet")
    ent = reader2.to_entry()
    ent.extract_parameter(name="index_key", value="beet")
    assert ent.user_parameters["index_key"].default == "beet"

    assert str(ent.to_dict()).count("{index_key}") == 2  # once in select, once in set_index
    assert intake.readers.utils.descend_to_path("steps.1.2.keys", ent.kwargs) == "{index_key}"

    assert ent.to_reader() == reader2

    cat = intake.readers.entry.Catalog()
    cat.add_entry(reader2)
    datadesc = list(cat.data.values())[0]
    datadesc.extract_parameter(name="protocol", value="memory:")
    assert datadesc.kwargs["url"] == dataframe_file.replace("memory:", "{protocol}")
    datadesc.user_parameters["protocol"].set_default("env(TEMP_TEST)")
    monkeypatch.setenv("TEMP_TEST", "memory:")
    out = datadesc.to_data()
    assert out == data


def test_namespace(dataframe_file):
    data = intake.readers.datatypes.CSV(url=dataframe_file)
    reader = intake.readers.readers.PandasCSV(data)
    assert "np" in reader._namespaces
    assert reader.apply(getattr, "beet").np.max().read() == 3
