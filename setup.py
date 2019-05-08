# -*- coding: utf-8 -*-
"""
    Copyright (2019) Haensel AMS GmbH & Inc

    HAMS Data API

"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='hams_data_api',
    version='0.0.1',
    description='A HAMS public toolkit for interacting with HAMS databases.',
    long_description=long_description,
    long_description_content_type='text/markdown',  # Optional (see note above)
    author='Haensel AMS GmbH & Inc',  # Optional
    author_email='info@haensel-ams.com',  # Optional

    keywords='HaenselAMS Data API Toolkit',

    packages=find_packages(exclude=['contrib', 'tests']),

    install_requires=['pymysql', 'pandas', 'numpy'],

    include_package_data=True


)
