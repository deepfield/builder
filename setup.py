#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='builder',
      version='0.2.0',
      description='A framework for building batch processing pipelines',
      author='Max Mizikar, Matt Hollingsworth',
      url='http://github.com/deepfield/builder',
      packages=find_packages(),
      install_requires=['python-dateutil', 'arrow>=0.4.3']
)
