import sys

try:
    from setuptools import setup
except:
    sys.exit('Requires distribute or setuptools')

from setuptools import find_packages

setup(
    name = 'pywqp',
    version = '0.1.4-dev',
    description = 'Interface to the Water Quality Portal',
    long_description = open('README.md').read(),
    license='Public Domain',
    maintainer='William Blondeau',
    maintainer_email='wblondeau@usgs.gov',
    py_modules=['pywqp'],
    packages=find_packages(),
    install_requires=['setuptools'],
    url='https://github.com/wblondeau-usgs/pywqp',
    test_suite='tests',
)
