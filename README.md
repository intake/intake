# Intake: A general interface for loading data

![Logo](https://github.com/intake/intake/raw/master/logo-small.png)

[![Build Status](https://github.com/intake/intake/workflows/CI/badge.svg)](https://github.com/intake/intake/actions)
[![Documentation Status](https://readthedocs.org/projects/intake/badge/?version=latest)](http://intake.readthedocs.io/en/latest/?badge=latest)
[![Join the chat at https://gitter.im/ContinuumIO/intake](https://badges.gitter.im/ContinuumIO/intake.svg)](https://gitter.im/ContinuumIO/intake?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


Intake is a lightweight set of tools for loading and sharing data in data science projects.
Intake helps you:

* Load data from a variety of formats (see the [current list of known plugins](http://intake.readthedocs.io/en/latest/plugin-directory.html)) into containers you already know, like Pandas dataframes, Python lists, NumPy arrays, and more.
* Convert boilerplate data loading code into reusable Intake plugins
* Describe data sets in catalog files for easy reuse and sharing between projects and with others.
* Share catalog information (and data sets) over the network with the Intake server

Documentation is available at [Read the Docs](http://intake.readthedocs.io/en/latest).

Status of intake and related packages is available at [Status Dashboard](https://intake.github.io/status)

Weekly news about this repo and other related projects can be found on the
[wiki](https://github.com/intake/intake/wiki/Community-News)

Install
-------

Recommended method using conda:
```bash
conda install -c conda-forge intake
```

You can also install using `pip`, in which case you have a choice as to how many of the optional
dependencies you install, with the simplest having least requirements

```bash
pip install intake
```

and additional sections `[server]`, `[plot]` and `[dataframe]`, or to include everything:

```bash
pip install intake[complete]
```

Note that you may well need specific drivers and other plugins, which usually have additional
dependencies of their own.

Development
-----------
 * Create development Python environment with the required dependencies, ideally with `conda`.
   The requirements can be found in the yml files in the `scripts/ci/` directory of this repo.
   * e.g. `conda env create -f scripts/ci/environment-py38.yml` and then `conda activate test_env`
 * Install intake using `pip install -e .[complete]`
 * Use `pytest` to run tests.
 * Create a fork on github to be able to submit PRs.
 * We respect, but do not enforce, pep8 standards; all new code should be covered by tests.
