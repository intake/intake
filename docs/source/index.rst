.. raw:: html

   <img src="_static/images/logo.png" alt="Intake Logo" style="float:right;width:94px;height:60px;">

Intake
======

*Taking the pain out of data access and distribution*

Intake is a lightweight package for finding, investigating, loading and sharing data. 

Data User
---------

.. raw:: html

   <img src="_static/images/line.png" alt="Intake Logo" style="float:left;width:160px;height:120px;">



Intake helps you:

* Load data from a variety of formats (see :ref:`plugin-directory`) into containers you already use,
  like Pandas dataframes, Python lists, NumPy arrays, and more.
* Read data catalogs and actual data from remote/cloud storage
* Translate your boilerplate data loading code into reusable Intake plugins
* Describe data sets in catalog files for easy reuse and sharing between projects and with others.
* Share catalog information (and data sets) over the network with the Intake server

The philosophy of Intake is to be extremely pluggable, so that new plugins, new data service catalogs,
new auth
mechanisms, etc., are easy to implement. Thus, with community support, we can make a single catalog
and data interface
for the python data ecosystem and take the pain out of data access. Get involved!

New users are encouraged to check out the Quickstart and executable Examples.


.. toctree::
    :maxdepth: 2
    :caption: Contents:

    quickstart.rst
    overview.rst
    examples.rst
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
    glossary.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
