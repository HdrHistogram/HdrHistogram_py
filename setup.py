'''
Setup
'''
import sys
from setuptools import setup
from setuptools import Extension
from setuptools.command.test import test


class Tox(test):
    def initialize_options(self):
        test.initialize_options(self)
        self.tox_args = None # pylint: disable=attribute-defined-outside-init

    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True # pylint: disable=attribute-defined-outside-init

    def run_tests(self):
        import tox # pylint: disable=import-outside-toplevel
        sys.exit(tox.cmdline())


if __name__ == '__main__':
    setup(setup_requires=['pbr'], pbr=True,
          keywords='hdrhistogram hdr histogram high dynamic range',
          tests_require=['tox'],
          cmdclass={'test': Tox},
          ext_modules=[Extension('pyhdrh',
                                 sources=['src/python-codec.c'])]
          )
