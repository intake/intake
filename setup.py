#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Data load and catalog system',
    url='https://github.com/ContinuumIO/intake',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    package_data={'': ['*.csv', '*.yml', '*.html']},
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
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    python_requires=">=3.5",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    zip_safe=False,
)
