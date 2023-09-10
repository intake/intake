from intake.readers import entry


def test_yaml_roundtrip():
    cat = entry.Catalog()
    cat["one"] = entry.ReaderDescription(entry.DataDescription("intake.readers.datatypes:BaseData"), "intake.readers.readers:BaseReader")
    cat.to_yaml_file("memory://cat.yaml")
    cat2 = entry.Catalog.from_yaml_file("memory://cat.yaml")
    assert cat == cat2
