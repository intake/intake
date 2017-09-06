#!/usr/bin/env python

import os
from setuptools import setup, find_packages
import sys
import versioneer


requires = open('requirements.txt').read().strip().split('\n')
install_requires = []

setup(name='intake',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Data input plugin system',
      url='https://github.com/ContinuumIO/intake',
      maintainer='Stan Seibert',
      maintainer_email='sseibert@anaconda.com',
      license='BSD',
      package_data={ '': ['*.csv', '*.yml'], },
      include_package_data=True,
      install_requires=install_requires,
      packages=find_packages(),
      long_description=open('README.rst').read(),
)