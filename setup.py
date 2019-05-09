#! /usr/bin/env python
"""Package for interacting with pandas extension modules.

Any additional repos that may require client-side libs to do
data manipulation.
"""
from setuptools import find_packages, setup

import codecs
import os
import re


###############################################################
NAME = "pandas_ext"
PACKAGES = find_packages(exclude=('tests', 'docs'))
META_PATH = os.path.join("pandas_ext", "__init__.py")
CLASSIFIERS = [
    'Development Status :: 3 - Alpha',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 2.7',
]
##############################################################

HERE = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), "rb", "utf-8") as f:
        return f.read()


README = read('README.md')
META_FILE = read(META_PATH)


def find_meta(meta):
    """
    Extract __*meta*__ from META_FILE.
    """

    meta_match = re.search(
        r"^__{meta}__ = ['\"]([^'\"]*)['\"]".format(meta=meta), META_FILE, re.M
    )
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError(f"Unable to find __{meta}__ string.")


setup(
    name=NAME,
    description=find_meta("description"),
    author=find_meta("author"),
    author_email=find_meta("email"),
    url=find_meta("uri"),
    version=find_meta("version"),
    license=find_meta("license"),
    install_requires=read("requirements/requirements.in"),
    extras_require=dict(
        xls=["xlwt"],
        xlsx=["openpyxl", "xlsxwriter"],
        snowflake=["setuptools>=41.0.1", "snowflake-sqlalchemy"],
        parquet=["pyarrow"],
        ),
    long_description=README,
    packages=PACKAGES,
    classifiers=CLASSIFIERS,
)
