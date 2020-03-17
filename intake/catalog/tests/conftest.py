import os.path
import pytest
from intake import open_catalog


@pytest.fixture
def catalog1():
    path = os.path.dirname(__file__)
    return open_catalog(os.path.join(path, 'catalog1.yml'))
