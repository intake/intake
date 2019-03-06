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
    cat_browser = CatSelector()
    cat_browser.add(cat1)
    return cat_browser

@pytest.fixture
def sources(cat1):
    return list(cat1._entries.values())

@pytest.fixture
def source1(sources):
    return sources[0]

@pytest.fixture
def source_browser(sources):
    from ..source_select import SourceSelector
    source_browser = SourceSelector()
    source_browser.add(sources)
    return source_browser
