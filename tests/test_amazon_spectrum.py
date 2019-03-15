"""Test suite for amazon functions."""
import json

from os import getenv

import pandas as pd
import pytest
import requests

from pandas_ext.amazon_spectrum import to_spectrum
from pandas_ext.sqla_utils import schema_from_registry


@pytest.fixture(scope='module')
def df():
    return pd.DataFrame(
        dict(
            a=[1, 2, 3],
            b=[4, 5, 6],
            c=[7, 8, 9]
        )
    )


def test_has_partition(df):
    assert 'PARTITIONED BY' in to_spectrum(
        df,
        'test',
        'my-schema',
        'my-bucket',
        has_partition=True,
        )


def test_has_no_partition(df):
    assert 'PARTITIONED BY' not in to_spectrum(
            df,
        'test',
        'my-schema',
        'my-bucket',
        has_partition=False,
        )


def test_schema_alias(df):
    assert 'CREATE VIEW' in to_spectrum(
            df,
        'test',
        'my-schema',
        'my-bucket',
        schema_alias='my-new-schema',
        )


def test_schema_registry():

    def create_test_stream(stream):
        endpoint = getenv('NES_SCHEMAS_ENDPOINT')
        key = getenv('NES_SCHEMAS_KEY')
        url = f'{endpoint}/create_schema/schema_driver/redshift_spectrum/schema/{stream}'
        data = dict(
                columns=[dict(name='id', type='VARCHAR(36)')],
                user_id='richardfernandeznyc@gmail.com')
        requests.post(
                url,
                headers={'x-api-key': key},
                json=data
                )
    create_test_stream('test')
    schema = schema_from_registry('test')
    assert 'id' in schema
