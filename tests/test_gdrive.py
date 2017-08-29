# from collections import namedtuple
from os import path
from tempfile import gettempdir

import pytest

from pandas_ext import gdrive


@pytest.fixture
def url():
    """Valid test Url."""
    return 'https://drive.google.com/open?id=0BysWz9wJTD4KOTVvWkVKY3E4QTQ'


@pytest.fixture
def to_gdrive_params():
    return dict(
        file_name='test - (1).csv',
        folder_id='0B_HIHwXpt9XJMUJvUlJMM0hmTTA',
        data=None,
        to_gapp=False
    )


def test_metadata(url):
    """Happy path."""
    metadata = gdrive.gdrive_metadata(url)
    assert metadata.title == 'animals.csv'


def test_metadata_fetch_all(url):
    metadata = gdrive.gdrive_metadata(url, fetch_all=True)
    assert isinstance(metadata, dict)


def test_gdrive_writes(to_gdrive_params):
    file_name = to_gdrive_params['file_name']
    with open(path.join(gettempdir(), file_name), 'w') as test_file:
        test_file.write(file_name)
    result = gdrive.to_gdrive(**to_gdrive_params)
    assert result.endswith('edit?usp=drivesdk')
