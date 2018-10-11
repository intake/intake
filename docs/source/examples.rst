Examples
========

Here we list links to notebooks and other code demonstrating the use of Intake in various
scenarios. The first section is of general interest to various users, and the sections that
follow tend to be more specific about particular features and workflows.

Many of the entries here include a link to Binder, which a service that lest you execute
code live in a notebook environment. This is a great way to experience using Intake.
It can take a while, sometimes, for Binder to come up, please have patience.

See also the `example data`_ page, containing data-sets which can be built and installed
as conda packages.

.. _example data: https://github.com/ContinuumIO/intake/tree/master/examples


General
-------

- Basic Data scientist workflow: using Intake
  [`Static <https://github.com/martindurant/intake-release-blog/blob/master/data_scientist.ipynb>`_]
  [`Executable <https://mybinder.org/v2/gh/martindurant/intake-release-blog/master?filepath=data_scientist.ipynb>`_].

- Workflow for creating catalogs: a Data Engineer's approach to Intake
  [`Static <https://github.com/martindurant/intake-release-blog/blob/master/data_engineer.ipynb>`_]
  [`Executable <https://mybinder.org/v2/gh/martindurant/intake-release-blog/master?filepath=data_engineer.ipynb>`_]

Developer
---------

Tutorials delving deeper into the Internals of Intake, for those who wish to contribute

- How you would go about writing a new plugin
  [`Static <https://github.com/martindurant/intake-release-blog/blob/master/dev.ipynb>`_]
  [`Executable <https://mybinder.org/v2/gh/martindurant/intake-release-blog/master?filepath=dev.ipynb>`_]

Features
--------

More specific examples of Intake functionality

- Caching:

    - Using automatically cached of data-files
      [`Static <https://github.com/mmccarty/intake-blog/blob/master/examples/caching.ipynb>`_]
      [`Executable <https://mybinder.org/v2/gh/mmccarty/intake-blog/master?filepath=examples%2Fcaching.ipynb>`_]

    - Earth science demonstration of cached dataset
      [`Static <https://github.com/mmccarty/intake-blog/blob/master/examples/Walker_Lake.ipynb>`_]
      [`Executable <https://mybinder.org/v2/gh/mmccarty/intake-blog/master?filepath=examples%2FWalker_Lake.ipynb>`_]

- File-name pattern parsing:

    - Satellite imagery, science workflow
      [`Static <https://github.com/jsignell/intake-blog/blob/master/path-as-pattern/landsat.ipynb>`_]
      [`Executable <https://mybinder.org/v2/gh/jsignell/intake-blog/master?filepath=path-as-pattern%2Flandsat.ipynb>`_]

    - How to set up pattern parsing
      [`Static <https://github.com/jsignell/intake-blog/blob/master/path-as-pattern/csv.ipynb>`_]
      [`Executable <https://mybinder.org/v2/gh/jsignell/intake-blog/master?filepath=path-as-pattern%2Fcsv.ipynb>`_]

Blogs
-----

These are Intake-related articles that may be of interest.

- `Taking the Pain out of Data Access`_
- `Caching Data on First Read Makes Future Analysis Faster`_
- `Parsing Data from Filenames and Paths`_

.. _Taking the Pain out of Data Access: https://www.anaconda.com/blog/developer-blog/intake-taking-the-pain-out-of-data-access/
.. _Caching Data on First Read Makes Future Analysis Faster: https://www.anaconda.com/blog/developer-blog/intake-caching-data-on-first-read-makes-future-analysis-faster/
.. _Parsing Data from Filenames and Paths: https://www.anaconda.com/blog/developer-blog/intake-parsing-data-from-filenames-and-paths/
