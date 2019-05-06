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
    return create_engine(
        URL(
            account=account,
            user=user,
            password=password,
            database=database,
            warehouse=warehouse,
            **kwargs
        )
    )


def from_snowflake(sql: str, con=None, **kwargs) -> pd.DataFrame:
    """Retrieves a query from snowflake."""
    if con is None:
        con = connect()
    return pd.read_sql(sql, **kwargs)


def to_snowflake(
        df,
        schema,
        table,
        con=None,
        index=False,
        if_exists='append',
        **kwargs
) -> pd.DataFrame:
    """Sends the current dataframe to snowflake."""
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
    connect()
