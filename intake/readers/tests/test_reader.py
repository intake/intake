import tempfile

import pytest

import intake


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
