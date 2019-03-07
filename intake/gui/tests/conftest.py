import os
import pytest
import intake

here = os.path.abspath(os.path.dirname(__file__))

@pytest.fixture
def cat1_url():
    return os.path.join(here, '..', '..', 'catalog', 'tests', 'catalog1.yml')

@pytest.fixture
def cat2_url():
    return os.path.join(here, '..', '..', 'catalog', 'tests', 'catalog_union_2.yml')

@pytest.fixture
def cat1(cat1_url):
    return intake.open_catalog(cat1_url)

@pytest.fixture
def cat2(cat2_url):
    return intake.open_catalog(cat2_url)

@pytest.fixture
def cat_browser(cat1):
    from ..source_select import CatSelector
    return CatSelector(cats=[cat1])

@pytest.fixture
def sources1(cat1):
    return list(cat1._entries.values())

@pytest.fixture
def sources2(cat1):
    return list(cat1._entries.values())

@pytest.fixture
def source_browser(sources1):
    from ..source_select import SourceSelector
    return SourceSelector(sources=sources1)
