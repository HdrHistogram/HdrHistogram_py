import sys

from setuptools import setup
from setuptools.command.test import test


class Tox(test):
    def initialize_options(self):
        test.initialize_options(self)
        self.tox_args = None

    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import tox
        sys.exit(tox.cmdline())

__version__ = '0.1.0'

if __name__ == '__main__':
    setup(
        name='hdrhistogram',
        version=__version__,
        description='High Dynamic Range histogram in native python',
        url='https://github.com/ahothan/hdrhistogram',
        download_url='https://github.com/ahothan/hdrhistogram/tarball/' + __version__,
        author='Alec Hothan',
        author_email='ahothan@gmail.com',
        license='Apache 2.0',

        packages=['hdrh'],
        keywords='hdrhistogram hdr histogram high dynamic range',
        tests_require=['tox'],
        cmdclass={'test': Tox}
    )
