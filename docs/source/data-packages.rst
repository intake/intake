Making Data Packages
====================

Combined with the `Conda Package Manger <https://conda.io/docs/>`_, Intake makes it possible to create *data packages*
which can be installed and upgraded just like software packages.  This offers several advantages:

  * Distributing datasets becomes as easy as installing software
  * Data packages can be versioned, improving reproducibility in some cases
  * Data packages can depend on the libraries required for reading
  * Data packages can be self-describing using Intake catalog files
  * Applications that need certain datasets can include data packages in their dependency list

In this tutorial, we give a walkthrough to enable you to distribute any dataset to others, so that they can access the
data using Intake without worrying about where it resides or how it should be loaded.

Defining a Package
''''''''''''''''''

The steps involved in creating a data package are:

1. Identifying a dataset, which can be accessed via a URL or included directly as one or more files in the package.

2. Creating a package containing an intake catalog file, meta.yaml (description of the data, version, requirements,
   etc.) and a script to copy the data.

3. Building the package using the command conda build.

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
    
    mkdir -p $PREFIX/share/intake/civilservices
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
''''''''''''''

Versioning
----------

* Versions for data packages should be used to indicate changes in the data values or schema.  This allows applications
  to easily pin to the specific data version they depend on.

* Package build numbers should be used to indicate changes in the packaging of the data (fixes to conda package
  metadata, like dependencies).  If you need to change the data format (like CSV to Parquet), this can be indicated
  with a new build number, but only if the data contents and schema are identical even after the format change.
  (When in doubt, assign a new version number.)

* Putting data files into a package ensures reproducibility by allowing a version number to be associated with files
  on disk.  This can consume quite a bit of disk space for the user, however.  Conda does use hard-links when
  installing packages into an environment, so the disk space used by a data package will not multiply as it is added
  to more environments in the same Anaconda installation.

Packaging
---------

* Packages that refer to remote data sources (such as databases and REST APIs) need to think about authentication.
  Do not include authentication credentials inside a data package.  They should be obtained from the environment.

* Data packages should depend on the Intake plugins required to read the data, or Intake itself.

* Although it is technically possible to embed plugin code into a data package, this is discouraged.  It is better to
  break that code out into a separate package so that it can be updated independent of the data.

* Anaconda Cloud accounts have disk usage limits, so be careful uploading data packages there.  You may want to host
  them on a separate web server or cloud storage bucket.
  `conda index <https://conda.io/docs/commands/build/conda-index.html>`_ will help you construct the required JSON
  metadata to host conda packages.
