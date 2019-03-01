import os
import pytest
import intake

here = os.path.abspath(os.path.dirname(__file__))
cat1_url = os.path.join(here, '..', '..', 'catalog', 'tests', 'catalog1.yml')
cat2_url = os.path.join(here, '..', '..', 'catalog', 'tests', 'catalog_union_2.yml')

@pytest.fixture
def cat1():
    return intake.open_catalog(cat1_url)

@pytest.fixture
def cat2():
    return intake.open_catalog(cat2_url)

@pytest.fixture
def cat_browser(cat1):
    from ..source_select import CatSelector
    cat_browser = CatSelector()
    cat_browser.add(cat1)
    return cat_browser

@pytest.fixture
def sources(cat1):
    return list(cat1.values())

@pytest.fixture
def source_browser(sources):
    from ..source_select import SourceSelector
    source_browser = SourceSelector()
    source_browser.add(sources)
    return source_browser
