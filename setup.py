#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='builder',
      version='0.1.0',
      description='A framework for building batch processing pipelines',
      author='Max Mizikar, Matt Hollingsworth',
      url='http://github.com/deepfield/builder',
      packages=find_packages(),
      install_requires=['python-dateutil', 'arrow>=0.4.3', 'networkx>=1.8.1', 'futures>=2.1.6', 'tornado >= 3.2.2']
)
