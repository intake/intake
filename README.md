# Intake: A general interface for loading data

![Logo](https://github.com/intake/intake/raw/master/logo-small.png)

[![Build Status](https://travis-ci.org/intake/intake.svg?branch=master)](https://travis-ci.org/intake/intake)
[![Coverage Status](https://coveralls.io/repos/github/intake/intake/badge.svg?branch=master)](https://coveralls.io/github/intake/intake?branch=master)
[![Documentation Status](https://readthedocs.org/projects/intake/badge/?version=latest)](http://intake.readthedocs.io/en/latest/?badge=latest)
[![Join the chat at https://gitter.im/ContinuumIO/intake](https://badges.gitter.im/ContinuumIO/intake.svg)](https://gitter.im/ContinuumIO/intake?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Waffle.io - Columns and their card count](https://badge.waffle.io/intake/intake.svg?columns=all)](https://waffle.io/intake/intake)


Intake is a lightweight set of tools for loading and sharing data in data science projects.
Intake helps you:

* Load data from a variety of formats (see the [current list of known plugins](http://intake.readthedocs.io/en/latest/plugin-directory.html)) into containers you already know, like Pandas dataframes, Python lists, NumPy arrays, and more.
* Convert boilerplate data loading code into reusable Intake plugins
* Describe data sets in catalog files for easy reuse and sharing between projects and with others.
* Share catalog information (and data sets) over the network with the Intake server

Documentation is available at [Read the Docs](http://intake.readthedocs.io/en/latest).

Status of intake and related packages is available at [Status Dashboard](https://intake.github.io/status)

Install
-------

Recommended method using conda:
```bash
conda install -c conda-forge intake
```


Development Environment
----------------------------
 * Create development Python environment.
 * `pip install -r requirements.txt`
 * `python setup.py develop`
 * Verify development environment by running the unit tests with `py.test`.
