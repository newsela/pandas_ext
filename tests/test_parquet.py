#! /usr/bin/env python
import numpy as np
import pyarrow as pa
import pandas as pd

from os import path
from tempfile import mkdtemp

from pandas_ext import parquet


def test():
    df = pd.DataFrame(
        {'one': [-1, np.nan, 2.5],
         'two': ['foo', 'bar', 'baz'],
         'three': [True, False, True]
        }
    )

    table = pa.Table.from_pandas(df)

    dest = path.join(mkdtemp(), 'test.parquet')
    parquet.to_parquet(df, dest)

    df2 = parquet.read_parquet(dest)

    labels = ['one', 'two', 'three']
    assert list(df[labels].columns) == list(df2[labels].columns)
