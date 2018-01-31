import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pandas_ext.common.utils import fs


def to_parquet(df: pd.DataFrame, path: str, compression:str='snappy', **kwargs) -> None:
    """Convert dataframe to parquet file in path."""
    return pq.write_table(
        pa.Table.from_pandas(df),
        fs(path, 'wb'),
        compression=compression,
        flavor='spark',
        **kwargs
    )


def read_parquet(path:str, **kwargs) -> pd.DataFrame:
    """Read parquet table locally or from s3."""
    return pq.read_table(fs(path, 'rb'), **kwargs).to_pandas()
