#!/usr/bin/python

from setuptools import setup, find_packages
import sys

DESCRIPTION = """python tajo client
"""

install_requires = ["protobuf==2.5.0"]

setup(
    name="tajo-client",
    version="0.0.5",
    description="a Python implementation of Tajo Client",
    long_description=DESCRIPTION,
    url='http://github.com/charsyam/python-tajo-client/',
    author='DaeMyung Kang',
    author_email='charsyam@gmail.com',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    packages=find_packages('src', exclude=[
            '*.*.tests', '*.*.examples', '*.*.examples.*']),
    package_dir={'': 'src'},
    install_requires=install_requires,
    test_suite='tajo.tests',
)
