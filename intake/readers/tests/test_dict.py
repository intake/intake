from intake.readers import entry


def test_yaml_roundtrip():
    cat = entry.Catalog()
    cat["one"] = entry.ReaderDescription(entry.DataDescription("intake.readers.datatypes:BaseData"), "intake.readers.readers:BaseReader")
    cat.to_yaml_file("memory://cat.yaml")
    cat2 = entry.Catalog.from_yaml_file("memory://cat.yaml")
    assert cat2.user_parameters.pop("CATALOG_DIR")
    assert cat2.user_parameters.pop("STORAGE_OPTIONS") == {}
    assert cat == cat2
