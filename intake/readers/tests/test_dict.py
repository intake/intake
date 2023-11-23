import intake.readers
from intake.readers import entry


def test_yaml_roundtrip():
    cat = entry.Catalog()
    cat["one"] = intake.readers.BaseReader(intake.BaseData(), output_instance="blah")
    cat.to_yaml_file("memory://cat.yaml")
    cat2 = entry.Catalog.from_yaml_file("memory://cat.yaml")
    assert cat2.user_parameters.pop("CATALOG_DIR")
    assert cat2.user_parameters.pop("STORAGE_OPTIONS") == {}
    assert cat.data == cat2.data
    assert list(cat.entries) == list(cat2.entries)
    assert cat2["one"].output_instance == "blah"
