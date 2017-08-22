import pytest
from pandas_ext import gdrive


@pytest.fixture
def url():
    """Valid test Url."""
    return 'https://drive.google.com/open?id=0BysWz9wJTD4KOTVvWkVKY3E4QTQ'


def test_metadata(url):
    """Happy path."""
    metadata = gdrive.gdrive_metadata(url)
    assert isinstance(metadata, dict)
