import pytest

import intake.readers

pytest.importorskip("siphon")
pytest.importorskip("xarray")
pytest.importorskip("h5netcdf")


def test_1():
    req = pytest.importorskip("requests")
    u = "https://psl.noaa.gov/thredds/catalog.xml"
    try:
        req.head(u)
        assert req.ok
    except:
        pytest.xfail("server down")
    data = intake.readers.datatypes.THREDDSCatalog(url=u)
    ds = (
        data.to_reader()
        .THREDDSCatToMergedDataset(
            path="Datasets/ncep.reanalysis.dailyavgs/surface/air.sig995.194*.nc"
        )
        .read()
    )
    assert "1948-01-01" in str(ds.time.min())
    assert "1949-12-31" in str(ds.time.max())
