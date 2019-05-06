"""Versioning kept here."""
__version__ = '0.5.01'
__license__ = "MIT"

__title__ = "pandas_ext"
__description__ = "Python Pandas extensions for pandas dataframes"

__author__ = "Rich Fernandez, Sean Massot, Brian Tenazas, Luke Orland"
__email__ = "devs@newsela.com"

__uri__ = "https://github.com/newsela/pandas_ext"

import pandas as _pd
read_csv = _pd.read_csv
del _pd

from .amazon_spectrum import to_spectrum
from .px_csv import to_csv
from .excel import to_excel
from .gdrive import read_gdrive, to_gdrive
from .parquet import read_parquet, to_parquet
from .sfdc import (
    read_sfdc, sfdc_metadata, patch_sfdc, async_patch_sfdc,
)
from .sql import read_sql, list_backends
from .snowflake import read_snowflake, to_snowflake



# Don't pollute the namespace with the module names:
del amazon_spectrum
del common
del px_csv
del excel
del gdrive
del parquet
del sfdc
del sql
del snowflake
