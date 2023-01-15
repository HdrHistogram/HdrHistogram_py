'''
Setup
'''
import sys
from setuptools import setup
from setuptools import Extension
from setuptools.command.test import test


if __name__ == '__main__':
    setup(setup_requires=['pbr'], pbr=True,
          keywords='hdrhistogram hdr histogram high dynamic range',
          ext_modules=[Extension('pyhdrh',
                                 sources=['src/python-codec.c'])]
          )
