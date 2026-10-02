import fsspec
import intake.readers
from intake.readers import entry
from intake.readers.utils import pattern_to_glob
from intake.source.utils import reverse_formats


def test_yaml_roundtrip():
    cat = entry.Catalog()
    cat["one"] = intake.readers.BaseReader(intake.BaseData(), output_instance="blah")
    cat.to_yaml_file("memory://cat.yaml")
    cat2 = entry.Catalog.from_yaml_file("memory://cat.yaml")
    catalog_dir = cat2.user_parameters.pop("CATALOG_DIR")
    assert catalog_dir.startswith("memory://")
    assert cat2.user_parameters.pop("STORAGE_OPTIONS") == {}
    assert cat.data == cat2.data
    assert list(cat.entries) == list(cat2.entries)
    assert cat2["one"].output_instance == "blah"


def test_local_catalog_dir_has_no_file_protocol(tmp_path):
    """Local from_yaml_file must not put file:// on CATALOG_DIR (#894)."""
    catdir = tmp_path / "campaign"
    radar = catdir / "instruments" / "radar"
    radar.mkdir(parents=True)
    for hour in (1, 2):
        (radar / f"20260428_{hour:02d}0000.nc").touch()
    filename = catdir / "campaign.yaml"

    cat = entry.Catalog()
    cat["one"] = intake.readers.BaseReader(intake.BaseData(), output_instance="blah")
    cat.to_yaml_file(filename)
    cat2 = entry.Catalog.from_yaml_file(filename)

    catalog_dir = cat2.user_parameters["CATALOG_DIR"]
    assert "file://" not in catalog_dir

    # Same failure as XArrayPatternReader: reverse_format + make_path_posix
    # cwd-prefixes file:// and raises ValueError on the doubled path.
    pattern = f"{catalog_dir}/instruments/radar/20260428_{{time}}.nc"
    _, _, paths = fsspec.get_fs_token_paths(pattern_to_glob(pattern))
    assert reverse_formats(pattern, paths)["time"] == ["010000", "020000"]
