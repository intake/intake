import pytest
import intake
from intake.readers.utils import subclasses
from intake.readers.readers import FileReader


@pytest.mark.parametrize("cls", subclasses(intake.BaseReader))
def test_readers(cls):
    assert isinstance(cls.imports, set)
    assert all(isinstance(_, str) for _ in cls.imports)

    assert isinstance(cls.implements, set)
    assert all(issubclass(_, intake.BaseData) for _ in cls.implements)

    assert isinstance(cls.func, str)
    assert cls.func.count(":") == 1
    assert cls.func_doc is None or cls.func_doc.count(":") == 1
    assert isinstance(cls.output_instance, str) or cls.output_instance is None


@pytest.mark.parametrize("cls", subclasses(intake.BaseData))
def test_data(cls):
    assert isinstance(cls.filepattern, str)
    assert isinstance(cls.mimetypes, str)
    assert isinstance(cls.structure, set) and all(isinstance(s, str) for s in cls.structure)
    assert isinstance(cls.magic, set) and all(isinstance(m, (bytes, tuple)) for m in cls.magic)
    assert isinstance(cls.contains, set) and all(isinstance(c, str) for c in cls.contains)


@pytest.mark.parametrize("cls", subclasses(FileReader))
def test_filereaders(cls):
    assert isinstance(cls.url_arg, str)
