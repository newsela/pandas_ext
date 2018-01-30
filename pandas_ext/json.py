import pandas as pd

from typing import Union, Dict


def read_json(
    path: str,
    dtype:Union[bool, Dict[str, str]]=True,
    lines: bool=True,
    compression: str='infer',
    **kwargs
) -> pd.DataFrame:
    """Simplified read_json from pandas.
    
    dtype takes a bool to infer type or a dict of str, str pairs to map to
    valid pandas types.
    """
    return pd.read_json(
        path,
        dtype=dtype,
        lines=lines,
        compression=compression,
        **kwargs
    )
