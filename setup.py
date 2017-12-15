#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(name='MapReduce',
      version='1.0',
      description='Shad MapReduce',
      author='chiselko6',
      author_email='alex.limontov@gmail.com',
      packages=find_packages(),
      entry_points={
        'console_scripts': [
            'mr=mr.mr_runner:start',
        ]
    }
)
