# -*- coding: utf-8 -*-
from setuptools import setup

try:
    with open('requirements.txt') as f:
        required = f.read().splitlines()
except:
    pass

setup(name="carto-etl",
      author="Daniel Carrión, Alberto Romeu",
      author_email="daniel@cartodb.com, alrocar@cartodb.com",
      description="ETL and geocoding functions",
      version="1.0.0",
      url="https://github.com/CartoDB/carto-etl",
      install_requires=required,
      packages=["etl"])
