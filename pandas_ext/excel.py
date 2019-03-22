from os import path, remove
from tempfile import gettempdir

import pandas as pd
import s3fs


from pandas_ext.common.utils import is_s3_path


def to_excel(df: pd.DataFrame, file_path: str, **kwargs) -> None:
    """Given a df, write it to s3 if necessary."""
    if is_s3_path(file_path):
        ext = file_path.split('.')[-1].lower()
        engine = dict(xls='xlwt', xlsx='xlsxwriter')[ext]
        path_removed = file_path.split('/')[-1]
        tmp_file = path.join(gettempdir(), path_removed)
        with pd.ExcelWriter(tmp_file, engine=engine) as writer:
            print(tmp_file)
            df.to_excel(writer, **kwargs)
        # writer.save()

        s3 = s3fs.S3FileSystem()
        with s3.open(file_path, 'wb') as dest:
            return dest.write(open(tmp_file, 'rb').read())
        remove(tmp_file)

    return df.to_excel(path, **kwargs)
