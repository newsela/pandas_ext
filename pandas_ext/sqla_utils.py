"""
pandas dtype hierarchy.

[numpy.generic,
[[numpy.number,
[[numpy.integer,
[[numpy.signedinteger,
[numpy.int8,
numpy.int16,
numpy.int32,
numpy.int64,
numpy.int64,
numpy.timedelta64]],
[numpy.unsignedinteger,
[numpy.uint8,
numpy.uint16,
numpy.uint32,
numpy.uint64,
numpy.uint64]]]],
[numpy.inexact,
[[numpy.floating,
[numpy.float16, numpy.float32, numpy.float64, numpy.float128]],
[numpy.complexfloating,
[numpy.complex64, numpy.complex128, numpy.complex256]]]]]],
[numpy.flexible,
[[numpy.character, [numpy.bytes_, numpy.str_]],
[numpy.void, [numpy.record]]]],
numpy.bool_,
numpy.datetime64,
numpy.object_]]

redshift spectrum data types:
Data Type   Aliases     Description
SMALLINT    INT2    Signed two-byte integer
INTEGER     INT, INT4   Signed four-byte integer
BIGINT  INT8    Signed eight-byte integer
DECIMAL     NUMERIC     Exact numeric of selectable precision
REAL    FLOAT4  Single precision floating-point number
DOUBLE PRECISION    FLOAT8, FLOAT   Double precision floating-point number
BOOLEAN     BOOL    Logical Boolean (true/false)
CHAR    CHARACTER, NCHAR, BPCHAR    Fixed-length character string
VARCHAR     CHARACTER VARYING, NVARCHAR, TEXT   Variable-length character string
with a user-defined limit
DATE        Calendar date (year, month, day)
TIMESTAMP   TIMESTAMP WITHOUT TIME ZONE     Date and time (without time zone)
TIMESTAMPTZ     TIMESTAMP WITH TIME ZONE    Date and time (with time zone)
"""
from os import getenv

import numpy as np
import pandas as pd
import requests

from pandas.api.types import pandas_dtype


def dtype_to_spectrum(dtype):
    """convert pandas dtype to equivalent redshift spectrum schema column."""
    try:
        return {
            pandas_dtype(np.float64): 'FLOAT8',
            pandas_dtype(np.object): 'VARCHAR(8192)',
            pandas_dtype(np.int64): 'INT8',
            pandas_dtype(np.bool): 'BOOL',
            pandas_dtype(np.datetime64): 'TIMESTAMP',
            pandas_dtype('<M8[s]'): 'TIMESTAMP'
        }[dtype]
    except KeyError:
        return 'TEXT'


def schema_from_df(df: pd.DataFrame) -> str:
    """Get schema from a pandas DataFrame"""
    dtype_map = df.dtypes.to_dict()
    return ',\n'.join(
            [f'"{col}" {dtype_to_spectrum(dtype)}'
                for col, dtype in dtype_map.items()
            ]
    )


def schema_from_registry(stream: str) -> str:
    """Using NES schemas endpoint, pull the latest schema from the registry.
    
    NES schemas repo coming soon!
    """
    endpoint = getenv('NES_SCHEMAS_ENDPOINT')
    if not endpoint:
        return schema_from_df(stream)

    key = getenv('NES_SCHEMAS_KEY')
    url = f'{endpoint}/schema/{stream}/version/latest'
    response = requests.get(url, headers={'x-api-key': key}).json()
    parsed = response[0]['columns']
    return ',\n'.join(
            [f'''"{dct['name']}" {dct['type']}''' for dct in parsed]
            )
