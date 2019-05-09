import pandas as pd
from pandas_ext.snowflake import to_snowflake, read_snowflake, connect

def test_to_snowflake():
    df = pd.DataFrame(dict(a=[1, 2, 3], b=[4, 5, 6]))
    result = to_snowflake(df, 'test', 'test')
    assert result is None

def test_read_snowflake():
    df = read_snowflake('select a from test.test')
    assert len(df) > 0
