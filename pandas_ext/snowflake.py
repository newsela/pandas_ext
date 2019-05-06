"""Snowflake connection module."""
from os import getenv

import pandas as pd

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def connect(**kwargs):
    """Connect to snowflake."""
    user = getenv('SNOWFLAKE_USER')
    password = getenv('SNOWFLAKE_PASSWORD')
    account = getenv('SNOWFLAKE_ACCOUNT')
    database = getenv('SNOWFLAKE_DATABASE')
    warehouse = getenv('SNOWFLAKE_WAREHOUSE')
    role = getenv('SNOWFLAKE_ROLE')
    return create_engine(
        URL(
            account=account,
            user=user,
            password=password,
            database=database,
            warehouse=warehouse,
            role=role,
            **kwargs
        )
    )


def read_snowflake(sql: str, con=None, **kwargs) -> pd.DataFrame:
    """Retrieves a query from snowflake.
    
    >>> df = read_snowflake('select a from test.test')
    """
    if con is None:
        con = connect()
    return pd.read_sql(sql, con, **kwargs)


def to_snowflake(
        df,
        schema,
        table,
        con=None,
        index=False,
        if_exists='append',
        **kwargs
) -> pd.DataFrame:
    """Sends the current dataframe to snowflake.
    
    >>> df = pd.DataFrame(dict(a=[1,2,3],b=[4,5,6]))
    >>> to_snowflake(df, 'test', 'test')
    """
    if con is None:
        con = connect()
    df.to_sql(
        con=con,
        schema=schema,
        name=table,
        index=index,
        if_exists=if_exists,
        **kwargs
    )


if __name__ == '__main__':
    df = pd.DataFrame(dict(a=[1,2,3],b=[4,5,6]))
    to_snowflake(df, 'test', 'test',)
    test_df = read_snowflake('select a from test.test')
    print(test_df)
