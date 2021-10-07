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
  'server': ['tornado', 'python-snappy', 'msgpack-python'],
  'plot': ['hvplot', 'panel >= 0.7.0', 'bokeh'],
  'dataframe': ['dask[dataframe]', 'msgpack-numpy', 'pyarrow'],
  'remote': ['requests']
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
        ],
        'intake.drivers': [
            'yaml_file_cat = intake.catalog.local:YAMLFileCatalog',
            'yaml_files_cat = intake.catalog.local:YAMLFilesCatalog',
            'csv = intake.source.csv:CSVSource',
            'textfiles = intake.source.textfiles:TextFilesSource',
            'catalog = intake.catalog.base:Catalog',
            'intake_remote = intake.catalog.remote:RemoteCatalog',
            'numpy = intake.source.npy:NPySource',
            'ndzarr = intake.source.zarr:ZarrArraySource',
            'zarr_cat = intake.catalog.zarr:ZarrGroupCatalog',
            'alias = intake.source.derived:AliasSource',
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    tests_require=['pytest'],
    extras_require=extras_require,
    zip_safe=False,
)
