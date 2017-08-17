import os
from setuptools import find_packages, setup
from pandas_ext import __version__

README = open(os.path.join(os.path.dirname(__file__), 'README.md'),
              'r', encoding="utf-8").read()

with open(os.path.join(os.path.dirname(__file__),
                       'requirements.txt')) as f:
    required = f.read().splitlines()

with open(os.path.join(os.path.dirname(__file__),
                       'test_requirements.txt')) as f:
    test_required = f.read().splitlines()

with open('LICENSE') as f:
    license = f.read()

setup(
    name="pandas_ext",
    description="Python Pandas extensions for pandas dataframes",
    author="Rich Fernandez, Sean Massot, Brian Tenazas",
    author_email="dev@namely.com",
    url="https://github.com/namely/pandas_ext",
    version=__version__,
    license=license,
    install_requires=required,
    tests_requires=test_required,
    long_description=README,
    packages=find_packages(exclude=('tests', 'docs')),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 2.7',
    ],

)
