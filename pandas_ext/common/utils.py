import datetime

from functools import partial
from enum import auto, Enum
from os import path

import s3fs


class Mode(Enum):
    rb = auto()
    wb = auto()


def is_s3_path(path: str):
    return path.startswith('s3://')


def fs(path:str, mode:Mode='rb'):
    """Opens file locally or via s3 depending on path string."""
    s3 = s3fs.S3FileSystem()
    return partial(
        {True: s3.open, False: open}[is_s3_path(path)],
        mode=mode
    )(path)


def today():
    return str(datetime.date.today())

def now():
    return str(datetime.datetime.now())
