"""Save xls[x] files to s3."""
import os

from tempfile import gettempdir

import pandas as pd
import s3fs

from pandas_ext.common.utils import is_s3_path


def to_excel(df: pd.DataFrame, file_path: str, engine='', **kwargs) -> None:
    """Given a df, write it to s3 if necessary."""
    if is_s3_path(file_path):
        ext = file_path.split('.')[-1].lower()
        if not engine:
            engine = dict(xls='xlwt', xlsx='xlsxwriter')[ext]
        path_removed = file_path.split('/')[-1]
        tmp_file = os.path.join(gettempdir(), path_removed)
        with pd.ExcelWriter(tmp_file, engine=engine) as writer:
            df.to_excel(writer, **kwargs)

        s3 = s3fs.S3FileSystem()
        with open(tmp_file, 'rb') as source, s3.open(file_path, 'wb') as dest:
            dest.write(source.read())
        os.remove(tmp_file)
        return

    return df.to_excel(file_path, **kwargs)
