import os.path
import pytest
from intake import Catalog


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return Catalog(os.path.join(path, 'catalog1.yml'))
