Quickstart
==========

This guide will show you how to get started using Intake to read data.  It assumes you are working in either a conda or a virtualenv/pip environment.

Installation
------------

If you are using Anaconda or Miniconda, install Intake and a sample plugin with the following commands::

    conda install -c intake intake

If you are using virtualenv/pip, run the following commands (**does not work until Intake is on PyPI**)::

    pip install intake

Creating Sample Data
--------------------

Let's begin by creating a sample data set and catalog.  At the command line, run the ``intake example`` command.  This will create an example data catalog and CSV data file in the ``civilservices`` subdirectory.  This file contains some basic facts about US states.

Loading a Data Source
---------------------



Reading Data
------------



Working with Dask
-----------------


Opening a Catalog
-----------------


Installing Data Source Packages with Conda
------------------------------------------

Intake makes it possible to create conda packages that install data sources into a global (to the conda environment) catalog.  For example:


