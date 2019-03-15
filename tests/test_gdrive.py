# from collections import namedtuple
from os import path, environ
from tempfile import gettempdir

import pytest

from pandas_ext import gdrive


@pytest.fixture
def url():
    """Valid test Url."""
    return f"https://drive.google.com/open?id={environ['GDRIVE_TEST_ID']}"


@pytest.fixture
def to_gdrive_params():
    return dict(
        file_name='test - (1).csv',
        folder_id=environ['GDRIVE_TEST_FOLDER'],
        data=None,
        to_gapp=False
    )


@pytest.mark.skip(reason="Gdrive service not setup yet.")
def test_metadata(url):
    """Happy path."""
    metadata = gdrive.gdrive_metadata(url)
    assert metadata.title == 'animals.csv'


@pytest.mark.skip(reason="Gdrive service not setup yet.")
def test_metadata_fetch_all(url):
    metadata = gdrive.gdrive_metadata(url, fetch_all=True)
    assert isinstance(metadata, dict)


@pytest.mark.skip(reason="Gdrive service not setup yet.")
def test_gdrive_writes(to_gdrive_params):
    file_name = to_gdrive_params['file_name']
    with open(path.join(gettempdir(), file_name), 'w') as test_file:
        test_file.write(file_name)
    result = gdrive.to_gdrive(**to_gdrive_params)
    assert result.endswith('edit?usp=drivesdk')
