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
VARCHAR     CHARACTER VARYING, NVARCHAR, TEXT   Variable-length character string with a user-defined limit
DATE        Calendar date (year, month, day)
TIMESTAMP   TIMESTAMP WITHOUT TIME ZONE     Date and time (without time zone)
TIMESTAMPTZ     TIMESTAMP WITH TIME ZONE    Date and time (with time zone) 
"""
import pandas as pd
from pandas.api.types import pandas_dtype
import numpy as np


def dtype_to_spectrum(dtype):
    """convert pandas dtype to equivalent redshift spectrum schema column value."""
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


def schema_from_df(df: pd.DataFrame):
    dtype_map = df.dtypes.to_dict()
    return ',\n'.join(
            [f'"{col}" {dtype_to_spectrum(dtype)}'
                for col, dtype in dtype_map.items()
            ]
    )
