# Intake: A general interface for loading data
[![Build Status](https://travis-ci.org/ContinuumIO/intake.svg?branch=master)](https://travis-ci.org/ContinuumIO/intake)  [![Documentation Status](https://readthedocs.org/projects/intake/badge/?version=latest)](http://intake.readthedocs.io/en/latest/?badge=latest)

Intake is a lightweight set of tools for loading and sharing data in data science projects.  
Intake helps you:

* Load data from a variety of formats (see the [current list of known plugins](http://intake.readthedocs.io/en/latest/plugin-directory.html)) into containers you already know, like Pandas dataframes, Python lists, NumPy arrays, and more.
* Convert boilerplate data loading code into reusable Intake plugins
* Describe data sets in catalog files for easy reuse and sharing between projects and with others.
* Share catalog information (and data sets) over the network with the Intake server

Documentation is available at [Read the Docs](http://intake.readthedocs.io/en/latest).

Development Environment
----------------------------
 * Create development Python environment.
 * `pip install -r requirements.txt`
 * `python setup.py develop`
 * Verify development environment by running the unit tests with `py.test`.
