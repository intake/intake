#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from setuptools import setup, find_packages
import sys
import versioneer

requires = [line.strip() for line in open('requirements.txt').readlines()
            if not line.startswith("#")]
extras_require = {
  'server': ['tornado', 'python-snappy'],
  'plot': ['hvplot', 'panel >= 0.5.1'],
  'dataframe': ['dask[dataframe]', 'msgpack-numpy'],
}
extras_require['complete'] = sorted(set(sum(extras_require.values(), [])))

# Only include pytest-runner in setup_requires if we're invoking tests
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires = ['pytest-runner']
else:
    setup_requires = []

setup(
    name='intake',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Data load and catalog system',
    url='https://github.com/intake/intake',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    package_data={'': ['*.csv', '*.yml', '*.yaml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'intake-server = intake.cli.server.__main__:main',
            'intake = intake.cli.client.__main__:main'
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    python_requires=">=3.6",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    tests_require=['pytest'],
    extras_require=extras_require,
    zip_safe=False,
)
