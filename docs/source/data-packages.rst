Making Data Packages
====================

Intake can used to create :term:`Data packages`, so that you can easily distribute
your catalogs - others can just "install data". Since you may also want to distribute
custom catalogues, perhaps with visualisations, and driver code, packaging these things
together is a great convenience. Indeed, packaging gives you the opportunity to
version-tag your distribution and to declare the requirements needed to be able to
use the data. This is a common pattern for distributing code for python and other
languages, but not commonly seen for data artifacts.

The current version of Intake allows making data packages using standard python
tools (to be installed, for example, using ``pip``).
The previous, now deprecated, technique is still described below, under
:ref:`condapack` and is specific to the `conda` packaging system.

Python packaging solution
-------------------------

Intake allows you to register data artifacts (catalogs and data sources) in the
metadata of a python package. This means, that when you install that package, intake
will automatically know of the registered items, and they will appear within the
"builtin" catalog ``intake.cat``.

Here we assume that you understand what is meant by a python package (i.e., a
folder containing ``__init__.py`` and other code, config and data files).
Furthermore, you should familiarise yourself with what is required for
bundling such a package into a *distributable* package (one with a ``setup.py``)
by reading the `official packaging documentation`_

.. _official packaging documentation: https://packaging.python.org/tutorials/packaging-projects/

The `intake examples`_ contains a full tutorial for packaging and distributing
intake data and/or catalogs for ``pip`` and ``conda``, see the directory
"data_package/".

.. _intake examples: https://github.com/intake/intake-examples

Entry points definition
'''''''''''''''''''''''

Intake uses the concept of `entry points` to define the entries that are defined
by a given package. Entry points provide a mechanism to register metadata about a
package at install time, so that it can easily be found by other packages such as Intake.
Entry points was originally a `separate package`_, but is included in the standard
library as of python 3.8 (you will not need to install it, as Intake requires it).

All you need to do to register an entry in ``intake.cat`` is:

- define a data source somewhere in your package. This object can
  be of any ttype that makes sense to Intake, including Catalogs, and sources
  that have drivers defined in the very same package. Obviously, if you can have
  catalogs, you can populate these however you wish, including with more catalogs.
  You need not be restricted to simply loading in YAML files.
- include a block in your call to ``setp`` in ``setup.py`` with code something like

.. code-block:: python

    entry_points={
        'intake.catalogs': [
            'sea_cat = intake_example_package:cat',
            'sea_data = intake_example_package:data'
        ]
    }

  Here only the lines with "sea_cat" and "sea_data" are specific to the example
  package, the rest is required boilerplate. Each of those two lines defines a name
  for the data entry (before the "=" sign) and the location to load from, in
  module:object format.

- install the package using ``pip``, ``python setup.py``, or package it for ``conda``

.. _separate package: https://github.com/takluyver/entrypoints

Intake's process
''''''''''''''''

When Intake is imported, it investigates all registered entry points with the
``"intake.catalogs"`` group. It will go through and assign each name to the
given location of the final object. In the above example, ``intake.cat.sea_cat``
would be associated with the ``cat`` object in the ``intake_example_package``
package, and so on.

Note that Intake does **not** immediately import the given package or module, because imports
can sometimes be expensive, and if you have a lot of data packages, it might cause
a slow-down every time that Intake is imported. Instead, a placeholder entry is
created, and whenever the entry is accessed, that's when the particular package
will be imported.

.. code-block:: python

    In [1]: import intake

    In [2]: intake.cat.sea_cat  # does not import yet
    Out[2]: <Entry containing Catalog named sea_cat>

    In [3]: cat = intake.cat.sea_cat()  # imports now

    In [4]: cat   # this data source happens to be a catalog
    Out[4]: <Intake catalog: sea>

(note here the parentheses - this explicitly initialises the source, and normally
you don't have to do this)

.. _condapack:

Pure conda solution
-------------------

This packaging method is deprecated, but still available.

Combined with the `Conda Package Manger <https://conda.io/docs/>`_, Intake
makes it possible to create :term:`Data packages` which can be installed and upgraded just like
software packages.  This offers several advantages:

  * Distributing Catalogs and Drivers becomes as easy as ``conda install``
  * Data packages can be versioned, improving reproducibility in some cases
  * Data packages can depend on the libraries required for reading
  * Data packages can be self-describing using Intake catalog files
  * Applications that need certain Catalogs can include data packages in their dependency list

In this tutorial, we give a walk-through to enable you to distribute any
Catalogs to others, so that they can access the data using Intake without worrying about where it
resides or how it should be loaded.

Implementation
''''''''''''''

The function ``intake.catalog.default.load_combo_catalog`` searches for YAML catalog files in a number
of place at import. All entries in these catalogs are flattened and placed in the "builtin"
``intake.cat``.

The places searched are:

  * a platform-specific user directory as given by the `appdirs`_ package
  * in the environment's "/share/intake" data directory, where the location of the current environment
    is found from virtualenv or conda environment variables
  * in directories listed in the "INTAKE_PATH" environment variable or "catalog_path" config parameter

.. _appdirs: https://github.com/ActiveState/appdirs

Defining a Package
''''''''''''''''''

The steps involved in creating a data package are:

1. Identifying a dataset, which can be accessed via a URL or included directly as one or more files in the package.

2. Creating a package containing:

   * an intake catalog file
   * a ``meta.yaml`` file (description of the data, version, requirements, etc.)
   * a script to copy the data

3. Building the package using the command ``conda build``.

4. Uploading the package to a package repository such as `Anaconda Cloud <https://anaconda.org>`_ or your own private
   repository.

Data packages are standard conda packages that install an Intake catalog file into the user's conda environment
(``$CONDA_PREFIX/share/intake``).  A data package does not necessarily imply there are data files inside the package.
A data package could describe remote data sources (such as files in S3) and take up very little space on disk.

These packages are considered ``noarch`` packages, so that one package can be installed on any platform, with any
version of Python (or no Python at all).  The easiest way to create such a package is using a
`conda build <https://conda.io/docs/commands/build/conda-build.html>`_ recipe.

Conda-build recipes are stored in a directory that contains a files like:

  * ``meta.yaml`` - description of package metadata
  * ``build.sh`` - script for building/installing package contents (on Linux/macOS)
  * other files needed by the package (catalog files and data files for data packages)

An example that packages up data from a Github repository would look like this:

.. code-block:: yaml

    # meta.yaml
    package:
      version: '1.0.0'
      name: 'data-us-states'

    source:
      git_rev: v1.0.0
      git_url: https://github.com/CivilServiceUSA/us-states

    build:
      number: 0
      noarch: generic

    requirements:
      run:
        - intake
      build: []

    about:
      description: Data about US states from CivilServices (https://civil.services/)
      license: MIT
      license_family: MIT
      summary: Data about US states from CivilServices

The key parts of a data package recipe (different from typical conda recipes) is the ``build`` section:

.. code-block:: yaml

    build:
      number: 0
      noarch: generic

This will create a package that can be installed on any platform, regardless of the platform where the package is
built.  If you need to rebuild a package, the build number can be incremented to ensure users get the latest version when they conda update.

The corresponding ``build.sh`` file in the recipe looks like this:

.. code-block:: bash

    #!/bin/bash

    mkdir -p $CONDA_PREFIX/share/intake/civilservices
    cp $SRC_DIR/data/states.csv $PREFIX/share/intake/civilservices
    cp $RECIPE_DIR/us_states.yaml $PREFIX/share/intake/

The ``$SRC_DIR`` variable refers to any source tree checked out (from Github or other service), and the
``$RECIPE_DIR`` refers to the directory where the ``meta.yaml`` is located.

Finishing out this example, the catalog file for this data source looks like this:

.. code-block:: yaml

    sources:
      states:
        description: US state information from [CivilServices](https://civil.services/)
        driver: csv
        args:
          urlpath: '{{ CATALOG_DIR }}/civilservices/states.csv'
        metadata:
          origin_url: 'https://github.com/CivilServiceUSA/us-states/blob/v1.0.0/data/states.csv'

The ``{{ CATALOG_DIR }}`` Jinja2 variable is used to construct a path relative to where the catalog file was installed.

To build the package, you must have conda-build installed:

.. code-block:: bash

    conda install conda-build

Building the package requires no special arguments:

.. code-block:: bash

    conda build my_recipe_dir

Conda-build will display the path of the built package, which you will need to upload it.

If you want your data package to be publicly available on `Anaconda Cloud <https://anaconda.org>`_, you can install
the anaconda-client utility:

.. code-block:: bash

    conda install anaconda-client

Then you can register your Anaconda Cloud credentials and upload the package:

.. code-block:: bash

    anaconda login
    anaconda upload /Users/intake_user/anaconda/conda-bld/noarch/data-us-states-1.0.0-0.tar.bz2

Best Practices
--------------

Versioning
''''''''''

* Versions for data packages should be used to indicate changes in the data values or schema.  This allows applications
  to easily pin to the specific data version they depend on.

* Putting data files into a package ensures reproducibility by allowing a version number to be associated with files
  on disk.  This can consume quite a bit of disk space for the user, however. Large data files are not generally
  included in pip or conda packages so, if possible, you should reference the data assets in an external place where they
  can be loaded.

Packaging
'''''''''

* Packages that refer to remote data sources (such as databases and REST APIs) need to think about authentication.
  Do not include authentication credentials inside a data package.  They should be obtained from the environment.

* Data packages should depend on the Intake plugins required to read the data, or Intake itself.

* You may well want to break any driver code code out into a separate package so that it can be updated
  independent of the data. The data package would then depend on the driver package.

Nested catalogs
'''''''''''''''

As noted above, entries will appear in the users' builtin
catalog as ``intake.cat.*``. In the case that the catalog has multiple entries, it may be desirable
to put the entries below a namespace as ``intake.cat.data_package.*``. This can be achieved by having
one catalog containing the (several) data sources, with only a single top-level entry pointing to
it. This catalog could be defined in a YAML file, created using any other catalog driver, or constructed
in the code, e.g.:

.. code-block:: python

    from intake.catalog import Catalog
    from intake.catalog.local import LocalCatalogEntry as Entry
    cat = intake.catalog.Catalog()
    cat._entries = {name: Entry(name, descr, driver='package.module.driver',
                                  args={"urlpath": url})
                              for name, url in my_input_list}

If your package contains many sources of different types, you may even nest the catalogs, i.e.,
have a top-level whose contents are also catalogs.

.. code-block:: python

    e = Entry('first_cat', 'sample', driver='catalog')
    e._default_source = cat
    top_level = Catalog()
    top_level._entries = {'fist_cat': e, ...}

where your entry point might look something like: ``"my_cat = my_package:top_level"``. You could achieve the same
with multiple YAML files.
