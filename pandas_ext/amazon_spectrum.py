import pandas as pd
from pandas.io.sql import execute

from pandas_ext.common.utils import today, now
from pandas_ext.parquet import to_parquet
from pandas_ext.sqla_utils import schema_from_df


def _get_file_format_serde(file_format: str) -> str:
    return dict(
        parquet='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    )[file_format]


def _build_s3_stream_path(
    bucket,
    stream,
    file_format,
    partition,
    partition_value
):
    time_now = now()
    return (f's3://{bucket}/{stream}/ext={file_format}/'
            f'{partition}={partition_value}/{stream}_{time_now}'
    ).lower()


def _create_schema_alias_statement(
    schema_alias: str,
    schema: str,
    table: str
) -> str:
    return (
        f'CREATE VIEW "{schema_alias}"."{table}" AS\n'
        f'SELECT * FROM "{schema}"."{table}"\n'
        'WITH NO SCHEMA BINDING\n;\n'
    )


def _create_partition_statement(
    schema: str,
    bucket: str,
    stream: str,
    file_format: str='parquet',
    partition: str='dt',
    partition_value: str=''
) -> str:
    partition_value = (
        partition_value if partition_value else
        today()
    )
    s3_path = f's3://{bucket}/{stream}/ext={file_format}/{partition}={partition_value}/'.lower()
    return (
        f'ALTER TABLE {schema}.{stream}_{file_format}\n'
        f"ADD PARTITION ({partition}='{partition_value}')\n"
        f"LOCATION '{s3_path}'\n"
        ';\n'
    )


def _create_external_table_statement(
    schema: str,
    table: str,
    columns: str,
    bucket: str,
    stream: str,
    file_format: str='parquet',
    partition: str='dt',
    partition_type: str='date',
    partition_value: str=''
) -> str:
    serde = _get_file_format_serde(file_format)
    upper_file_format = file_format.upper()
    s3_path = f's3://{bucket}/{stream}/ext={file_format}/'.lower()

    return (
        f'CREATE EXTERNAL TABLE "{schema}"."{table}" (\n'
        f'{columns}\n'
        f')\nPARTITIONED BY ({partition} {partition_type})\n'
        f'ROW FORMAT SERDE {serde}\n'
        f'STORED AS {upper_file_format}\n'
        f"LOCATION '{s3_path}'\n"
        ';\n'
    )


def to_spectrum(
    df: pd.DataFrame,
    schema: str,
    table: str,
    bucket: str,
    schema_alias: str='',
    stream: str='',
    file_format: str='parquet',
    partition: str='dt',
    partition_type: str='date',
    partition_value: str='',
    conn: str='',
    **kwargs
) -> str:
    """Sends your dataframe to Spectrum for use in Athena/Redshift/Looker/etc

       Currently we only print out the statements as only the owner of the
       external schema can actually run the CREATE EXTERNAL TABLE statement.

       df: pandas Dataframe
       schema: external table schema
       table: table name as it appears in Spectrum
       bucket: s3 bucket
       schema_alias: If you want to create an alternate path to your schema
       stream: Defaults to table if not provided.
       file_format: Defaults to parquet and may expand to avro.
       partition: Defaults to dt, which is short for the date.
       partition_type: The data type declaration of the partition value.
       partition_value: Defaults to todays date.
       conn: A valid sqlalchemy string to connect to spectrum.
       kwargs: kwargs you want to pass to `to_parquet()` call
    """

    columns = schema_from_df(df)
    stream = stream if stream else table
    external_table_statement = _create_external_table_statement(
        schema=schema,
        table=table,
        columns=columns,
        bucket=bucket,
        stream=stream,
        file_format=file_format,
        partition=partition,
        partition_type=partition_type
    )
    alias_statement = (
        '' if not schema_alias else
        _create_alias_statement(schema_alias, schema, table)
    )
    partition_value = (
        partition_value if partition_value else
        today()
    )
    partition_statement = _create_partition_statement(
        schema=schema,
        bucket=bucket,
        stream=stream,
        partition=partition,
        partition_value=partition_value
    )
    create_statement = (''.join([
        external_table_statement,
        alias_statement,
        partition_statement
    ]))
    print(create_statement)

    s3_path = _build_s3_stream_path(bucket, stream, file_format, partition, partition_value)
    to_parquet(df, s3_path, **kwargs)
    print(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
    print(f"df_{table} = read_parquet('{s3_path}')")

    if conn:
        execute(create_statement, conn)
