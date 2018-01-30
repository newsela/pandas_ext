import numpy as np
import pyarrow as pa
import pandas as pd

from os import path
from tempfile import mkdtemp

from pandas_ext import csv

def test():
    df = pd.DataFrame(
        {'one': [-1, np.nan, 2.5],
         'two': ['foo', 'bar', 'baz'],
         'three': [True, False, True]
        }
    )

    dest = path.join(mkdtemp(), 'test.csv')
    csv.to_csv(df, dest)

    df2 = pd.read_csv(dest)

    labels = ['one', 'two', 'three']
    assert list(df[labels].columns) == list(df2[labels].columns)
