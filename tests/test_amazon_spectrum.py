import pytest

from functools import partial


from pandas_ext.amazon_spectrum import to_spectrum
import pandas as pd


df = pd.DataFrame(
        dict(
            a=[1, 2, 3],
            b=[4, 5, 6],
            c=[7, 8, 9]
            )
        )


def test_noisy_verbosity(capsys):
    to_spectrum(df, 'test', 'my-schema', 'my-bucket', verbose=False)
    out, err = capsys.readouterr()
    assert 'SELECT COUNT(*)' not in out


def test_silent_verbosity(capsys):
    to_spectrum(df, 'test', 'my-schema', 'my-bucket', verbose=True)
    out, err = capsys.readouterr()
    assert 'SELECT COUNT(*)' in out


def test_has_partition():
    assert 'PARTITIONED BY' in to_spectrum(df, 'test', 'my-schema', 'my-bucket', has_partition=True)


def test_has_no_partition():
    assert 'PARTITIONED BY' not in to_spectrum(df, 'test', 'my-schema', 'my-bucket', has_partition=False)

def test_schema_alias():
    assert 'CREATE VIEW' in to_spectrum(df, 'test', 'my-schema', 'my-bucket', schema_alias='my-new-schema')
