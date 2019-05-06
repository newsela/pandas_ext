import pandas as pd
import s3fs

from pandas_ext.common.utils import is_s3_path


def to_csv(df: pd.DataFrame, path: str, **kwargs) -> None:
    """Given a df, write it to s3 if necessary."""
    if is_s3_path(path):
        bytes_to_write = df.to_csv(None, **kwargs).encode()

        s3 = s3fs.S3FileSystem()
        with s3.open(path, 'wb') as dest:
            return dest.write(bytes_to_write)

    return df.to_csv(path, **kwargs)
