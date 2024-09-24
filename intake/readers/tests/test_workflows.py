import os

import fsspec
import pytest

import intake.readers
from intake.readers import convert, readers, utils, entry

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
    reader = intake.readers.PandasCSV(data)
    reader2 = reader[["apple", "beet"]].set_index(keys="beet")
    cat = intake.entry.Catalog()
    cat["mydata"] = reader2

    assert cat.mydata.read().equals(df[["apple", "beet"]].set_index(keys="beet"))
    assert cat.mydata.discover().equals(df[["apple", "beet"]].set_index("beet")[:10])

    cat["eq"] = reader.equals(other=reader)
    assert reader.equals(other=reader).read() is True
    assert cat.eq.read() is True

    with pytest.raises(NotImplementedError):
        cat.delete("eq", recursive=True)
    cat.delete("eq")
    assert "eq" not in cat


def test_pipeline_steps(dataframe_file, df):
    data = intake.readers.datatypes.CSV(url=dataframe_file)
    reader = intake.readers.PandasCSV(data)
    reader2 = reader[["apple", "beet"]].set_index(keys="beet")
    stepper = reader2.read_stepwise()
    assert isinstance(stepper, convert.PipelineExecution)
    assert stepper.data is None
    assert stepper.next[0] == 0
    assert isinstance(stepper.step(), convert.PipelineExecution)
    assert stepper.next[0] == 1
    assert stepper.data is not None
    assert isinstance(stepper.step(), convert.PipelineExecution)
    out = stepper.step()
    assert out.equals(df[["apple", "beet"]].set_index(keys="beet"))

    stepper = reader2.read_stepwise(breakpoint=1)
    assert stepper.next[0] == 1
    assert stepper.data is not None
    out = stepper.cont()
    assert out.equals(df[["apple", "beet"]].set_index(keys="beet"))


def test_parameters(dataframe_file, monkeypatch):
    data = intake.readers.datatypes.CSV(url=dataframe_file)
    reader = readers.PandasCSV(data)
    reader2 = reader[["apple", "beet"]].set_index(keys="beet")
    ent = reader2.to_entry()
    ent.extract_parameter(name="index_key", value="beet")
    assert ent.user_parameters["index_key"].default == "beet"

    assert str(ent.to_dict()).count("{index_key}") == 2  # once in select, once in set_index
    assert utils.descend_to_path("steps.2.2.keys", ent.kwargs) == "{index_key}"

    assert ent.to_reader() == reader2

    cat = entry.Catalog()
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
    reader = readers.PandasCSV(data)
    assert "np" in reader._namespaces
    assert reader.apply(getattr, "beet").np.max().read() == 3


calls = 0


def fails(x):
    global calls
    if calls < 2:
        calls += 1
        raise RuntimeError
    return x


def test_retry(dataframe_file):
    from intake.readers.readers import Retry

    data = intake.readers.datatypes.CSV(url=dataframe_file)
    reader = readers.PandasCSV(data)
    pipe = Retry(reader.apply(fails), allowed_exceptions=(ValueError,))
    cat = entry.Catalog()
    cat["ret1"] = pipe

    pipe = Retry(reader.apply(fails), allowed_exceptions=(RuntimeError,))
    cat["ret2"] = pipe

    with pytest.raises(RuntimeError):
        cat["ret1"].read()
    assert calls == 1
    assert cat["ret2"].read() is not None
    assert calls > 1


def dir_non_empty(d):
    import os

    return os.path.exists(d) and os.path.isdir(d) and bool(os.listdir(d))


def test_custom_cache(dataframe_file, tmpdir, df):
    from intake.readers.readers import Condition, PandasCSV, PandasParquet, FileExistsReader

    fn = f"{tmpdir}/file.parquet"
    data = intake.readers.datatypes.CSV(url=dataframe_file)
    part = PandasCSV(data)

    output = part.PandasToParquet(url=fn).transform(PandasParquet)
    data2 = intake.readers.datatypes.Parquet(url=fn)
    cached = PandasParquet(data=data2)
    reader2 = Condition(cached, if_false=output, condition=FileExistsReader(data2))

    assert os.listdir(tmpdir) == []
    out = reader2.read()
    assert os.listdir(tmpdir)

    assert df.equals(out)

    m = fsspec.filesystem("memory")
    m.rm(dataframe_file)

    with pytest.raises(IOError):
        # file has gone
        part.read()

    # but condition still picks read from cache
    out = reader2.read()
    assert df.equals(out)
