#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer

core_requires = ['ruamel.yaml >= 0.15.0', 'jinja2', 'appdirs', 'six', 
                 'numpy', 'pandas',
                 'dask[array,bag,dataframe,delayed] >= 0.17.0']

extra_requires = {
    'plotting': ['holoviews'],
    'client': ['requests', 'python-snappy',
               'msgpack-python', 'msgpack-numpy'],
}
extra_requires['server'] = extra_requires['client'] + ['tornado >= 4.5.1']
extra_requires['complete'] = sorted(set(sum(extra_requires.values(), [])))

test_requires = ['pytest']

setup(
    name='intake',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Data input plugin system',
    url='https://github.com/ContinuumIO/intake',
    maintainer='Stan Seibert',
    maintainer_email='sseibert@anaconda.com',
    license='BSD',
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=core_requires,
    extra_requires=extra_requires,
    tests_requires=test_requires,
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'intake-server = intake.cli.server.__main__:main',
            'intake = intake.cli.client.__main__:main'
        ]
    },
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    zip_safe=False,
)
