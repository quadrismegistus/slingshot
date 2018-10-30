import setuptools
from setuptools import setup
import os
import sys
_here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='mpi-slingshot',
    version='0.1-rc-1',
    description=('SLINGSHOT: Python wrapper for MPI to "slingshot" a small Python or R function against the Goliath of Big Data'),
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Ryan Heuser',
    author_email='heuser@stanford.edu',
    url='https://github.com/quadrismegistus/mpi-slingshot',
    license='MPL-2.0',
    packages=setuptools.find_packages(),
    install_requires=[
        #'click==6.7',
        #'numpy==1.13.1',
        #'pyyaml==3.12',
    ],
    scripts=['slingshot'],
    include_package_data=True,
    classifiers=[
        #'Development Status :: 3 - Alpha',
        #'Intended Audience :: Science/Research',
        #'Programming Language :: Python :: 2.7',
        #'Programming Language :: Python :: 3.6'
    ],
    )
