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

.. _example data: https://github.com/intake/intake/tree/master/examples


General
-------

- Basic Data scientist workflow: using Intake
  [`Static <https://github.com/martindurant/intake-release-blog/blob/master/data_scientist.ipynb>`__]
  [`Executable <https://mybinder.org/v2/gh/martindurant/intake-release-blog/master?filepath=data_scientist.ipynb>`__].

- Workflow for creating catalogs: a Data Engineer's approach to Intake
  [`Static <https://github.com/martindurant/intake-release-blog/blob/master/data_engineer.ipynb>`__]
  [`Executable <https://mybinder.org/v2/gh/martindurant/intake-release-blog/master?filepath=data_engineer.ipynb>`__]

Developer
---------

Tutorials delving deeper into the Internals of Intake, for those who wish to contribute

- How you would go about writing a new plugin
  [`Static <https://github.com/martindurant/intake-release-blog/blob/master/dev.ipynb>`__]
  [`Executable <https://mybinder.org/v2/gh/martindurant/intake-release-blog/master?filepath=dev.ipynb>`__]

Features
--------

More specific examples of Intake functionality

- Caching:

    - Using automatically cached of data-files
      [`Static <https://github.com/mmccarty/intake-blog/blob/master/examples/caching.ipynb>`__]
      [`Executable <https://mybinder.org/v2/gh/mmccarty/intake-blog/master?filepath=examples%2Fcaching.ipynb>`__]

    - Earth science demonstration of cached dataset
      [`Static <https://github.com/mmccarty/intake-blog/blob/master/examples/Walker_Lake.ipynb>`__]
      [`Executable <https://mybinder.org/v2/gh/mmccarty/intake-blog/master?filepath=examples%2FWalker_Lake.ipynb>`__]

- File-name pattern parsing:

    - Satellite imagery, science workflow
      [`Static <https://github.com/jsignell/intake-blog/blob/master/path-as-pattern/landsat.ipynb>`__]
      [`Executable <https://mybinder.org/v2/gh/jsignell/intake-blog/master?filepath=path-as-pattern%2Flandsat.ipynb>`__]

    - How to set up pattern parsing
      [`Static <https://github.com/jsignell/intake-blog/blob/master/path-as-pattern/csv.ipynb>`__]
      [`Executable <https://mybinder.org/v2/gh/jsignell/intake-blog/master?filepath=path-as-pattern%2Fcsv.ipynb>`__]

Blogs
-----

These are Intake-related articles that may be of interest.

- `Taking the Pain out of Data Access`_
- `Caching Data on First Read Makes Future Analysis Faster`_
- `Parsing Data from Filenames and Paths`_
- `Intake for cataloguing Spark`_
- `Intake released on Conda-Forge`_

.. _Intake for cataloguing Spark: https://www.anaconda.com/intake-for-cataloging-spark/
.. _Taking the Pain out of Data Access: https://www.anaconda.com/intake-taking-the-pain-out-of-data-access/
.. _Caching Data on First Read Makes Future Analysis Faster: https://www.anaconda.com/intake-caching-data-on-first-read-makes-future-analysis-faster/
.. _Parsing Data from Filenames and Paths: https://www.anaconda.com/intake-parsing-data-from-filenames-and-paths/
.. _Intake released on Conda-Forge: https://www.anaconda.com/intake-released-on-conda-forge/

Talks
-----

- `PyData DC (November 2018)`_
- `PyData NYC (October 2018)`_
- `ESIP tech dive (November 2018)`_

.. _ESIP tech dive (November 2018): https://www.youtube.com/watch?v=PSD7r3JFml0&feature=youtu.be
.. _PyData DC (November 2018): https://www.youtube.com/watch?v=OvZFtePHKXw
.. _PyData NYC (October 2018): https://www.youtube.com/watch?v=pjkMmJQfTb8