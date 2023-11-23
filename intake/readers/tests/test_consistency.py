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
    assert isinstance(cls.output_instance, str) or cls.output_instance is None


@pytest.mark.parametrize("cls", subclasses(FileReader))
def test_filereaders(cls):
    assert isinstance(cls.url_arg, str)
