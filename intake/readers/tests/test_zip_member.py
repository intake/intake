import zipfile

import pytest

import intake
from intake.readers import datatypes, readers


@pytest.mark.parametrize("archive_name", ["archive.zip", "archive.parquet"])
@pytest.mark.parametrize(
    "member, payload, expected, unexpected",
    [
        ("table.csv", b"value\n1\n2\n", datatypes.CSV, datatypes.Parquet),
        ("nested/settings.toml", b'key = "value"\n', datatypes.TOML, datatypes.Parquet),
    ],
)
def test_recommend_zip_member_filetype(
    tmp_path, archive_name, member, payload, expected, unexpected
):
    archive = tmp_path / archive_name
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(member, payload)
    url = f"zip://{member}::{archive}"

    recommended = datatypes.recommend(url, head=True)
    assert recommended[0] is expected
    assert unexpected not in recommended


@pytest.mark.parametrize("archive_name", ["archive.zip", "archive.parquet"])
def test_recommend_zip_csv_reader(tmp_path, monkeypatch, archive_name):
    pd = pytest.importorskip("pandas")
    archive = tmp_path / archive_name
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("table.csv", "value\n1\n2\n")
    url = f"zip://table.csv::{archive}"
    storage_options = {"zip": {"mode": "r"}}
    calls = []
    read_csv = pd.read_csv

    def recording_read_csv(**kwargs):
        calls.append(kwargs)
        return read_csv(**kwargs)

    monkeypatch.setattr(pd, "read_csv", recording_read_csv)
    datatype = intake.recommend(url, storage_options=storage_options)[0]
    assert datatype is datatypes.CSV
    reader = datatype(url, storage_options=storage_options).to_reader(outtype="pandas:DataFrame")
    assert isinstance(reader, readers.PandasCSV)
    assert reader.data.url == url
    assert reader.data.storage_options is storage_options
    assert reader.read()["value"].tolist() == [1, 2]
    assert calls == [{"filepath_or_buffer": url, "storage_options": storage_options}]
    assert calls[0]["storage_options"] is storage_options
    assert storage_options == {"zip": {"mode": "r"}}


def test_zip_member_does_not_match_service_url(tmp_path):
    archive = tmp_path / "archive.zip"
    member = "https://fixture.test/table.csv"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr(member, "value\n1\n2\n")

    recommended = datatypes.recommend(f"zip://{member}::{archive}")
    assert recommended[0] is datatypes.CSV
    assert datatypes.CatalogAPI not in recommended
    assert datatypes.TiledService not in recommended


def test_zip_member_failed_head_keeps_original_path(tmp_path, monkeypatch):
    from fsspec.implementations.zip import ZipFileSystem

    archive = tmp_path / "archive.parquet"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("table.csv", "value\n1\n2\n")

    def unavailable(*args, **kwargs):
        raise IOError("Fixture cannot read the member")

    monkeypatch.setattr(ZipFileSystem, "cat_file", unavailable)
    recommended = datatypes.recommend(f"zip://table.csv::{archive}")
    assert recommended[0] is datatypes.Parquet
    assert datatypes.CSV not in recommended


@pytest.mark.parametrize("head", [False, None, b"value\n1\n2\n"])
def test_zip_member_without_sniff_keeps_original_path(monkeypatch, head):
    import fsspec

    def unexpected_io(*args, **kwargs):
        pytest.fail("Filename-only inference must not construct a filesystem")

    monkeypatch.setattr(fsspec.core, "url_to_fs", unexpected_io)
    recommended = datatypes.recommend("zip://table.csv::archive.parquet", head=head)
    assert recommended[0] is datatypes.Parquet
    assert datatypes.CSV not in recommended
