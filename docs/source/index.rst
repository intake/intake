Intake: A general interface for loading data
============================================

Intake is a lightweight set of tools for loading and sharing data in data science projects.
Intake helps you:

* Load data from a variety of formats (see :ref:`plugin-directory`) into containers you already use, like Pandas dataframes, Python lists, NumPy arrays, and more.
* Read data catalogs and actual data from remote/cloud storage
* Translate your boilerplate data loading code into reusable Intake plugins
* Describe data sets in catalog files for easy reuse and sharing between projects and with others.
* Share catalog information (and data sets) over the network with the Intake server

The philosophy of Intake is to be extremely pluggable, so that new plugins, new data service catalogs, new auth
mechanisms, etc., are easy to implement. Thus, with community support, we can make a single catalog and data interface
for the python data ecosystem and take the pain out of data access. Get involved!

.. toctree::
    :maxdepth: 1
    :caption: Contents:

    quickstart.rst
    overview.rst
    gui.rst
    api_base.rst
    api_user.rst
    catalog.rst
    tools.rst
    making-plugins.rst
    auth-plugins.rst
    data-packages.rst
    plotting.rst
    plugin-directory.rst
    roadmap.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
