import tempfile

import pytest

import intake
from intake.readers import datatypes
from intake.readers.readers import (
    AwkwardAVRO,
    BaseReader,
    DaskNPYStack,
    DeltaReader,
    DuckCSV,
    DuckJSON,
    DuckParquet,
    GeoPandasReader,
    GeoPandasTabular,
    KerasImageReader,
    NumpyText,
    PandasCSV,
    PandasExcel,
    PandasFeather,
    PandasHDF5,
    PandasORC,
    PandasParquet,
    PanelImageViewer,
    PolarsAvro,
    PolarsExcel,
    SKImageReader,
    XArrayPatternReader,
    recommend,
)


def test_reader_from_call():
    import pandas as pd

    df = pd.DataFrame(
        {
            "col1": ["a", "b"],
            "col2": [1.0, 3.0],
        },
        columns=["col1", "col2"],
    )
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        df.to_csv(fp.name)
        fp.close()
        reader = intake.reader_from_call("df = pd.read_csv(fp.name)")
        read_df = reader.read()
        assert all(read_df.col1 == df.col1)
        assert all(read_df.col2 == df.col2)


@pytest.fixture()
def xarray_dataset():
    xr = pytest.importorskip("xarray")
    import numpy as np
    import pandas as pd

    temperature = 15 + 8 * np.random.randn(2, 3, 4)
    lon = [-99.83, -99.32]
    lat = [42.25, 42.21]
    instruments = ["manufac1", "manufac2", "manufac3"]
    time = pd.date_range("2014-09-06", periods=4)
    return xr.Dataset(
        data_vars=dict(
            temperature=(["loc", "instrument", "time"], temperature),
        ),
        coords=dict(
            lon=("loc", lon),
            lat=("loc", lat),
            instrument=instruments,
            time=time,
        ),
    )


def test_xarray_pattern(tmpdir, xarray_dataset):
    import numpy as np
    from intake.readers.readers import XArrayPatternReader

    if np.__version__.split(".") > ["2"]:
        pytest.skip("HDF does not yet support numpy 2")
    pytest.importorskip("h5netcdf")
    path1 = f"{tmpdir}/1.nc"
    path2 = f"{tmpdir}/2.nc"
    xarray_dataset.to_netcdf(path1)
    xarray_dataset.to_netcdf(path2)

    data = intake.datatypes.HDF5("%s/{part}.nc" % tmpdir)
    reader = XArrayPatternReader(data)
    ds = reader.read()

    assert ds.part.values.tolist() == ["1", "2"]
    assert ds.temperature.shape == (2, 2, 3, 4)

    data = intake.datatypes.HDF5("%s/{part:d}.nc" % tmpdir)
    reader = XArrayPatternReader(data)
    ds = reader.read()

    assert ds.part.values.tolist() == [1, 2]


def test_xarray_dataset_remote_url_glob_str(tmpdir, xarray_dataset):
    """Test opening HDF5 data with Xarray via remote URL with glob.

    We're using tar archive to create a remote URL, since this was the
    case that raised Issue #879.
    """
    from pathlib import Path
    import shutil

    from intake.readers.readers import XArrayDatasetReader

    pytest.importorskip("h5netcdf")

    root_dir = Path(tmpdir)

    path = root_dir / "test.nc"
    xarray_dataset.to_netcdf(path)

    # make archive.tar.gz in tmpdir
    tarname = f"{tmpdir}/archive"
    tarpath = shutil.make_archive(tarname, "gztar", root_dir=root_dir, base_dir="test.nc")

    data_url = "tar://*.nc::" + tarpath
    data = intake.datatypes.HDF5(data_url)
    reader = XArrayDatasetReader(data)
    ds = reader.read()

    # check that result is not empty
    assert ds.sizes.get("time", 0) > 0


@pytest.fixture
def icechunk_xr_repo(tmpdir):
    xr = pytest.importorskip("xarray")
    icechunk = pytest.importorskip("icechunk")
    pd = pytest.importorskip("pandas")
    import numpy as np

    np.random.seed(0)
    temperature = 15 + 8 * np.random.randn(2, 3, 4)
    precipitation = 10 * np.random.rand(2, 3, 4)
    lon = [-99.83, -99.32]
    lat = [42.25, 42.21]
    instruments = ["manufac1", "manufac2", "manufac3"]
    time = pd.date_range("2014-09-06", periods=4)
    reference_time = pd.Timestamp("2014-09-05")
    ds = xr.Dataset(
        data_vars=dict(
            temperature=(["loc", "instrument", "time"], temperature),
            precipitation=(["loc", "instrument", "time"], precipitation),
        ),
        coords=dict(
            lon=("loc", lon),
            lat=("loc", lat),
            instrument=instruments,
            time=time,
            reference_time=reference_time,
        ),
        attrs=dict(description="Weather related data."),
    )
    storage = icechunk.local_filesystem_storage(tmpdir.strpath)
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    store = session.store
    ds.to_zarr(store, consolidated=False)
    session.commit("Initial commit")
    return tmpdir.strpath


def test_icechunk(icechunk_xr_repo):
    data = intake.readers.datatypes.IcechunkRepo(
        "local_filesystem", storage_options={"path": icechunk_xr_repo}, ref="main"
    )
    reader = intake.readers.XArrayDatasetReader(data)
    ds = reader.read()
    assert (~ds.temperature.isnull()).all()


# ---------------------------------------------------------------------------
# Tests for BaseReader.is_ok and its interaction with recommend()
# ---------------------------------------------------------------------------


def test_base_reader_is_ok_always_true():
    """BaseReader.is_ok returns True for any data instance by default."""
    data = datatypes.FileData("some/file.csv")
    assert BaseReader.is_ok(data) is True


def test_is_ok_can_exclude_reader_from_recommend():
    """A reader whose is_ok() returns False is excluded from recommend() results."""

    class _AlwaysRejectReader(BaseReader):
        implements = {datatypes.CSV}
        output_instance = "builtins:object"

        @classmethod
        def is_ok(cls, data) -> bool:
            return False

    data = datatypes.CSV("some/file.csv")
    result = recommend(data)
    all_readers = result["importable"] + result["not_importable"]
    assert _AlwaysRejectReader not in all_readers


def test_is_ok_does_not_filter_when_passing_type():
    """When recommend() receives a class (not an instance) is_ok is not called."""

    class _AlwaysRejectReader(BaseReader):
        implements = {datatypes.CSV}
        output_instance = "builtins:object"

        @classmethod
        def is_ok(cls, data) -> bool:
            return False

    # Pass the class, not an instance — is_ok should be skipped entirely so
    # that type-only matching still works.
    result = recommend(datatypes.CSV)
    all_readers = result["importable"] + result["not_importable"]
    assert _AlwaysRejectReader in all_readers


# ---------------------------------------------------------------------------
# Tests for XArrayPatternReader.is_ok
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        # glob strings are accepted
        ("/data/*.nc", True),
        ("s3://bucket/dir/*.nc", True),
        # pattern strings with braces are accepted
        ("/data/file_{year}.nc", True),
        ("/data/{site}/{year}.nc", True),
        # lists / tuples of paths are accepted
        (["/data/a.nc", "/data/b.nc"], True),
        (("/data/a.nc",), True),
        # single plain filename — not a multi-file pattern, must be rejected
        ("/data/single.nc", False),
        ("s3://bucket/data.nc", False),
        # empty list — nothing to open
        ([], False),
    ],
)
def test_xarray_pattern_reader_is_ok(url, expected):
    data = datatypes.HDF5(url)
    assert XArrayPatternReader.is_ok(data) is expected


def test_xarray_pattern_reader_excluded_for_single_file():
    """recommend() should not include XArrayPatternReader for a single plain filename."""
    data = datatypes.HDF5("/data/single.nc")
    result = recommend(data)
    all_readers = result["importable"] + result["not_importable"]
    assert XArrayPatternReader not in all_readers


def test_xarray_pattern_reader_included_for_glob():
    """recommend() should include XArrayPatternReader when URL is a glob string."""
    data = datatypes.HDF5("/data/*.nc")
    result = recommend(data)
    all_readers = result["importable"] + result["not_importable"]
    assert XArrayPatternReader in all_readers


def test_xarray_pattern_reader_included_for_list():
    """recommend() should include XArrayPatternReader when URL is a list of paths."""
    data = datatypes.HDF5(["/data/a.nc", "/data/b.nc"])
    result = recommend(data)
    all_readers = result["importable"] + result["not_importable"]
    assert XArrayPatternReader in all_readers


# ---------------------------------------------------------------------------
# Helpers on FileReader
# ---------------------------------------------------------------------------


class TestFileReaderHelpers:
    def test_single_plain_path(self):
        data = datatypes.CSV("/tmp/data.csv")
        assert PandasCSV._url_is_single(data) is True
        assert PandasCSV._url_is_multi(data) is False

    def test_single_remote_url(self):
        data = datatypes.CSV("s3://bucket/data.csv")
        assert PandasCSV._url_is_single(data) is True

    def test_glob_string(self):
        data = datatypes.CSV("/tmp/*.csv")
        assert PandasCSV._url_is_single(data) is False
        assert PandasCSV._url_is_multi(data) is True

    def test_pattern_with_braces(self):
        data = datatypes.CSV("/tmp/data_{year}.csv")
        assert PandasCSV._url_is_single(data) is False

    def test_list_url(self):
        data = datatypes.CSV(["/tmp/a.csv", "/tmp/b.csv"])
        assert PandasCSV._url_is_single(data) is False
        assert PandasCSV._url_is_multi(data) is True


# ---------------------------------------------------------------------------
# Pandas family: single file only
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reader_cls,data",
    [
        (PandasCSV, datatypes.CSV("/tmp/data.csv")),
        (PandasParquet, datatypes.Parquet("/tmp/data.parquet")),
        (PandasFeather, datatypes.ArrowIPC("/tmp/data.feather")),
        (PandasORC, datatypes.ORC("/tmp/data.orc")),
        (PandasExcel, datatypes.Excel("/tmp/data.xlsx")),
    ],
)
def test_pandas_is_ok_single_file(reader_cls, data):
    assert reader_cls.is_ok(data) is True


@pytest.mark.parametrize(
    "reader_cls,data",
    [
        (PandasCSV, datatypes.CSV("/tmp/*.csv")),
        (PandasCSV, datatypes.CSV(["/tmp/a.csv", "/tmp/b.csv"])),
        (PandasParquet, datatypes.Parquet("/tmp/*.parquet")),
        (PandasParquet, datatypes.Parquet(["/tmp/a.parquet", "/tmp/b.parquet"])),
        (PandasFeather, datatypes.ArrowIPC("/tmp/*.feather")),
        (PandasORC, datatypes.ORC("/tmp/*.orc")),
        (PandasExcel, datatypes.Excel("/tmp/*.xlsx")),
    ],
)
def test_pandas_is_ok_multi_rejected(reader_cls, data):
    assert reader_cls.is_ok(data) is False


# ---------------------------------------------------------------------------
# PandasHDF5: local path only (no remote, no glob, no list)
# ---------------------------------------------------------------------------


def test_pandas_hdf5_is_ok_local_path():
    assert PandasHDF5.is_ok(datatypes.HDF5("/tmp/data.h5")) is True


@pytest.mark.parametrize(
    "url",
    [
        "s3://bucket/data.h5",
        "https://example.com/data.h5",
        "/tmp/*.h5",
        "/tmp/data_{year}.h5",
    ],
)
def test_pandas_hdf5_is_ok_rejects_non_local(url):
    assert PandasHDF5.is_ok(datatypes.HDF5(url)) is False


def test_pandas_hdf5_is_ok_rejects_list():
    assert PandasHDF5.is_ok(datatypes.HDF5(["/tmp/a.h5", "/tmp/b.h5"])) is False


# ---------------------------------------------------------------------------
# DuckDB family: single string URL only
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reader_cls,data",
    [
        (DuckCSV, datatypes.CSV("/tmp/data.csv")),
        (DuckCSV, datatypes.CSV("s3://bucket/data.csv")),
        (DuckParquet, datatypes.Parquet("/tmp/data.parquet")),
        (DuckJSON, datatypes.JSONFile("/tmp/data.json")),
    ],
)
def test_duck_is_ok_single(reader_cls, data):
    assert reader_cls.is_ok(data) is True


@pytest.mark.parametrize(
    "reader_cls,data",
    [
        (DuckCSV, datatypes.CSV("/tmp/*.csv")),
        (DuckCSV, datatypes.CSV(["/tmp/a.csv", "/tmp/b.csv"])),
        (DuckParquet, datatypes.Parquet("/tmp/*.parquet")),
        (DuckJSON, datatypes.JSONFile("/tmp/*.json")),
    ],
)
def test_duck_is_ok_rejects_multi(reader_cls, data):
    assert reader_cls.is_ok(data) is False


def test_duck_sql_no_url_passes():
    """DuckSQL operates on SQLQuery which has no 'url' attribute — must not raise."""
    data = datatypes.SQLQuery(conn={}, query="SELECT 1")
    from intake.readers.readers import DuckSQL

    assert DuckSQL.is_ok(data) is True


# ---------------------------------------------------------------------------
# DaskNPYStack: single directory path
# ---------------------------------------------------------------------------


def test_dask_npy_stack_is_ok_single_dir():
    assert DaskNPYStack.is_ok(datatypes.NumpyFile("/tmp/npy_stack")) is True


def test_dask_npy_stack_is_ok_rejects_glob():
    assert DaskNPYStack.is_ok(datatypes.NumpyFile("/tmp/*.npy")) is False


def test_dask_npy_stack_is_ok_rejects_list():
    assert DaskNPYStack.is_ok(datatypes.NumpyFile(["/tmp/a.npy", "/tmp/b.npy"])) is False


# ---------------------------------------------------------------------------
# KerasImageReader: single directory
# ---------------------------------------------------------------------------


def test_keras_image_reader_is_ok_single_dir():
    assert KerasImageReader.is_ok(datatypes.PNG("/tmp/images")) is True


def test_keras_image_reader_is_ok_rejects_glob():
    assert KerasImageReader.is_ok(datatypes.PNG("/tmp/*.png")) is False


def test_keras_image_reader_is_ok_rejects_list():
    assert KerasImageReader.is_ok(datatypes.PNG(["/tmp/a.png", "/tmp/b.png"])) is False


# ---------------------------------------------------------------------------
# SKImageReader / PanelImageViewer: single file
# ---------------------------------------------------------------------------


def test_skimage_reader_is_ok_single():
    assert SKImageReader.is_ok(datatypes.PNG("/tmp/img.png")) is True
    assert SKImageReader.is_ok(datatypes.TIFF("/tmp/img.tiff")) is True


def test_skimage_reader_is_ok_rejects_glob():
    assert SKImageReader.is_ok(datatypes.PNG("/tmp/*.png")) is False


def test_panel_image_viewer_is_ok_single():
    assert PanelImageViewer.is_ok(datatypes.PNG("/tmp/img.png")) is True


def test_panel_image_viewer_is_ok_rejects_list():
    assert PanelImageViewer.is_ok(datatypes.PNG(["/a.png", "/b.png"])) is False


# ---------------------------------------------------------------------------
# NumpyText family (NumpyReader, CupyNumpyReader, CupyTextReader): single file
# ---------------------------------------------------------------------------


def test_numpy_text_is_ok_single():
    from intake.readers.readers import NumpyReader

    assert NumpyText.is_ok(datatypes.FileData("/tmp/data.txt")) is True
    assert NumpyReader.is_ok(datatypes.NumpyFile("/tmp/data.npy")) is True


def test_numpy_text_is_ok_rejects_glob():
    from intake.readers.readers import NumpyReader

    assert NumpyText.is_ok(datatypes.FileData("/tmp/*.txt")) is False
    assert NumpyReader.is_ok(datatypes.NumpyFile("/tmp/*.npy")) is False


# ---------------------------------------------------------------------------
# PolarsAvro / PolarsExcel: single file (eager read_* not lazy scan_*)
# ---------------------------------------------------------------------------


def test_polars_avro_is_ok_single():
    assert PolarsAvro.is_ok(datatypes.AVRO("/tmp/data.avro")) is True


def test_polars_avro_is_ok_rejects_glob():
    assert PolarsAvro.is_ok(datatypes.AVRO("/tmp/*.avro")) is False


def test_polars_avro_is_ok_rejects_list():
    assert PolarsAvro.is_ok(datatypes.AVRO(["/tmp/a.avro", "/tmp/b.avro"])) is False


def test_polars_excel_is_ok_single():
    assert PolarsExcel.is_ok(datatypes.Excel("/tmp/data.xlsx")) is True


def test_polars_excel_is_ok_rejects_glob():
    assert PolarsExcel.is_ok(datatypes.Excel("/tmp/*.xlsx")) is False


# ---------------------------------------------------------------------------
# AwkwardAVRO: single file
# ---------------------------------------------------------------------------


def test_awkward_avro_is_ok_single():
    assert AwkwardAVRO.is_ok(datatypes.AVRO("/tmp/data.avro")) is True


def test_awkward_avro_is_ok_rejects_glob():
    assert AwkwardAVRO.is_ok(datatypes.AVRO("/tmp/*.avro")) is False


# ---------------------------------------------------------------------------
# GeoPandasReader / GeoPandasTabular: single file
# ---------------------------------------------------------------------------


def test_geopandas_reader_is_ok_single():
    assert GeoPandasReader.is_ok(datatypes.GeoJSON("/tmp/data.geojson")) is True
    assert GeoPandasReader.is_ok(datatypes.CSV("/tmp/data.csv")) is True


def test_geopandas_reader_is_ok_rejects_glob():
    assert GeoPandasReader.is_ok(datatypes.GeoJSON("/tmp/*.geojson")) is False


def test_geopandas_tabular_is_ok_single():
    assert GeoPandasTabular.is_ok(datatypes.Parquet("/tmp/data.parquet")) is True


def test_geopandas_tabular_is_ok_rejects_list():
    assert GeoPandasTabular.is_ok(datatypes.Parquet(["/a.parquet", "/b.parquet"])) is False


# ---------------------------------------------------------------------------
# DeltaReader: single table URI
# ---------------------------------------------------------------------------


def test_delta_reader_is_ok_single():
    assert DeltaReader.is_ok(datatypes.DeltalakeTable("/tmp/delta_table")) is True
    assert DeltaReader.is_ok(datatypes.Parquet("s3://bucket/delta_table")) is True


def test_delta_reader_is_ok_rejects_glob():
    assert DeltaReader.is_ok(datatypes.DeltalakeTable("/tmp/*.delta")) is False


def test_delta_reader_is_ok_rejects_list():
    assert DeltaReader.is_ok(datatypes.DeltalakeTable(["/a", "/b"])) is False


# ---------------------------------------------------------------------------
# Integration: recommend() respects is_ok for multi-file data
# ---------------------------------------------------------------------------


def test_recommend_excludes_single_only_readers_for_glob():
    """Readers that only handle single files must not appear for a glob URL."""
    data = datatypes.CSV("/tmp/*.csv")
    result = recommend(data)
    all_readers = result["importable"] + result["not_importable"]
    # Single-only readers must be absent
    assert PandasCSV not in all_readers
    assert DuckCSV not in all_readers
    assert GeoPandasReader not in all_readers


def test_recommend_excludes_single_only_readers_for_list():
    """Readers that only handle single files must not appear for a list URL."""
    data = datatypes.Parquet(["/tmp/a.parquet", "/tmp/b.parquet"])
    result = recommend(data)
    all_readers = result["importable"] + result["not_importable"]
    assert PandasParquet not in all_readers
    assert DuckParquet not in all_readers
    assert GeoPandasTabular not in all_readers
    assert DeltaReader not in all_readers


def test_recommend_includes_single_only_readers_for_plain_path():
    """Single-only readers must still appear for a plain single-file URL."""
    data = datatypes.CSV("/tmp/data.csv")
    result = recommend(data)
    all_readers = result["importable"] + result["not_importable"]
    assert PandasCSV in all_readers
    assert DuckCSV in all_readers
