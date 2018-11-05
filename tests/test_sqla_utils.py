#! /usr/bin/env python
import numpy as np
import pandas as pd

from collections import OrderedDict
from os import path
from tempfile import mkdtemp

from pandas_ext import sqla_utils


def test():
    data = OrderedDict([
        ('one',[-1, np.nan, 2.5]),
        ('two', ['foo', 'bar', 'baz']),
        ('three', [True, False, True])
    ])
    df = pd.DataFrame(data)
    
    assert sqla_utils.schema_from_df(df) == (
        '"one" FLOAT8,\n'
        '"two" VARCHAR(max),\n'
        '"three" BOOL'
    )
