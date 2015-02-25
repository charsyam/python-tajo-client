#!/usr/bin/python

from setuptools import setup, find_packages

DESCRIPTION = """python tajo client
"""

setup(
    name="tajo-client",
    version="0.0.1",
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
    # protobuf is not easy_install'able (yet) see
    # http://code.google.com/p/protobuf/issues/detail?id=66
    install_requires=['protobuf==2.5.0'],
    test_suite='tajo.tests',
)
