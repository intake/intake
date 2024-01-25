# Intake: Take 2

**A general python package for describing, loading and processing data**

![Logo](https://github.com/intake/intake/raw/master/logo-small.png)

[![Build Status](https://github.com/intake/intake/workflows/CI/badge.svg)](https://github.com/intake/intake/actions)
[![Documentation Status](https://readthedocs.org/projects/intake/badge/?version=latest)](http://intake.readthedocs.io/en/latest/?badge=latest)


*Taking the pain out of data access and distribution*

Intake is an open-source package to:

- describe your data declaratively
- gather data sets into catalogs
- search catalogs and services to find the right data you need
- load, transform and output data in many formats
- work with third party remote storage and compute platforms

Documentation is available at [Read the Docs](http://intake.readthedocs.io/en/latest).

Please report issues at https://github.com/intake/intake/issues

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

Note that you may well need specific drivers and other plugins, which usually have additional
dependencies of their own.

Development
-----------
 * Create development Python environment with the required dependencies, ideally with `conda`.
   The requirements can be found in the yml files in the `scripts/ci/` directory of this repo.
   * e.g. `conda env create -f scripts/ci/environment-py311.yml` and then `conda activate test_env`
 * Install intake using `pip install -e .`
 * Use `pytest` to run tests.
 * Create a fork on github to be able to submit PRs.
 * We respect, but do not enforce, pep8 standards; all new code should be covered by tests.
