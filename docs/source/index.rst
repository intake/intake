.. raw:: html

   <img src="_static/images/logo.png" alt="Intake Logo" style="float:right;width:94px;height:60px;">

Intake
======

*Taking the pain out of data access and distribution*

Intake is a lightweight package for finding, investigating, loading and disseminating data. It will appeal to different
groups for some of the reasons below, but is useful for all and acts as a common platform that everyone can use to
smooth the progression of data from developers and providers to users.


:term:`Data User`
-----------------

.. raw:: html

   <img src="_static/images/line.png" alt="Line graph" style="float:left;width:160px;height:120px;padding-right:25px">

* Intake loads the data for a range of formats and types (see :ref:`plugin-directory`) into containers you already use,
  like Pandas dataframes, Python lists, NumPy arrays, and more
* Intake loads, and gets out of your way
* GUI, search and introspect data-sets in :term:`Catalogs<Catalog>`: quickly find what you need to do your work
* Install data-sets and automatically get requirements
* Leverage cloud resources and distributed computing.

See the executable tutorial:

.. image:: https://mybinder.org/badge_logo.svg
   :target: https://mybinder.org/v2/gh/intake/intake-examples/master?filepath=tutorial%2Fdata_scientist.ipynb

:term:`Data Provider`
---------------------

.. raw:: html

   <img src="_static/images/grid.png" alt="Grid" style="float:right;width:160px;height:120px;">

* Simple spec to define data sources
* Single point-of truth, no more copy&paste
* Distribute data using packages, shared files or a server
* Update definitions in-place
* Parametrise user options
* Make use of additional functionality like filename parsing and caching.

See the executable tutorial:

.. image:: https://mybinder.org/badge_logo.svg
   :target: https://mybinder.org/v2/gh/intake/intake-examples/master?filepath=tutorial%2Fdata_engineer.ipynb

:term:`IT`
----------

.. raw:: html

   <img src="_static/images/terminal.png" alt="FA-terminal" style="float:right;width:80px;height:80px">

* Create catalogs out of established departmental practices
* Provide data access credentials via Intake parameters
* Use server-client architecture as gatekeeper:

   * add authentication methods
   * add monitoring point; track the data-sets being accessed.

* Hook Intake into proprietary data access systems.

:term:`Developer`
-----------------

.. raw:: html

   <img src="_static/images/code.png" alt="Python code" style="float:left;width:200px;height:90px;padding-right:25px">

* Turn boilerplate code into a reusable :term:`Driver`
* Pluggable architecture of Intake allows for many points to add and improve
* Open, simple code-base, come and get involved on `github`_!

.. _github: https://github.com/intake/intake


See the executable tutorial:

.. image:: https://mybinder.org/badge_logo.svg
   :target: https://mybinder.org/v2/gh/intake/intake-examples/master?filepath=tutorial%2Fdev.ipynb

First steps
===========

For a brief demonstration, which you can execute locally, go to :doc:`quickstart`.
For a general description of all of the components of Intake and how they fit together, fo
to :doc:`overview`. Finally, for some notebooks using Intake and articles about Intake, go
to :doc:`examples` and `intake-examples`_.
These and other documentation pages will make reference to concepts that
are defined in the :doc:`glossary`.

.. _intake-examples: https://github.com/intake/intake-examples

|

|

.. toctree::
    :maxdepth: 1
    :caption: Documentation Contents

    quickstart.rst
    use_cases.rst
    overview.rst
    examples.rst
    gui.rst
    api.rst
    catalog.rst
    tools.rst
    persisting.rst
    making-plugins.rst
    auth-plugins.rst
    data-packages.rst
    plotting.rst
    plugin-directory.rst
    roadmap.rst
    glossary.rst
    server.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
