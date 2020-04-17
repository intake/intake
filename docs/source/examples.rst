Examples
========

Here we list links to notebooks and other code demonstrating the use of Intake in various
scenarios. The first section is of general interest to various users, and the sections that
follow tend to be more specific about particular features and workflows.

Many of the entries here include a link to Binder, which a service that lest you execute
code live in a notebook environment. This is a great way to experience using Intake.
It can take a while, sometimes, for Binder to come up; please have patience.

See also the `examples`_ repository, containing data-sets which can be built and installed
as conda packages.

.. _examples: https://github.com/intake/intake-examples/

General
-------

- Basic Data scientist workflow: using Intake
  [`Static <https://github.com/intake/intake-examples/blob/master/tutorial/data_scientist.ipynb>`__]
  [`Executable <https://mybinder.org/v2/gh/intake/intake-examples/master?filepath=tutorial%2Fdata_scientist.ipynb>`__].

- Workflow for creating catalogs: a Data Engineer's approach to Intake
  [`Static <https://github.com/intake/intake-examples/blob/master/tutorial/data_engineer.ipynb>`__]
  [`Executable <https://mybinder.org/v2/gh/intake/intake-examples/master?filepath=tutorial%2Fdata_engineer.ipynb>`__]

Developer
---------

Tutorials delving deeper into the Internals of Intake, for those who wish to contribute

- How you would go about writing a new plugin
  [`Static <https://github.com/intake/intake-examples/blob/master/tutorial/dev.ipynb>`__]
  [`Executable <https://mybinder.org/v2/gh/intake/intake-examples/master?filepath=tutorial%2Fdev.ipynb>`__]

Features
--------

More specific examples of Intake functionality

- Caching:

    - New-style data package creation [`Static <https://github.com/intake/intake-examples/tree/master/data_package>`__]

    - Using automatically cached data-files
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

- Custom catalogs:

    - A custom intake plugin that adapts DCAT catalogs
      [`Static <https://github.com/CityOfLosAngeles/intake-dcat/blob/master/examples/demo.ipynb>`__]
      [`Executable <https://mybinder.org/v2/gh/CityOfLosAngeles/intake-dcat/master?urlpath=lab%2Ftree%2Fexamples%2Fdemo.ipynb>`__]


Data
----

- `Anaconda package data`_, originally announced in `this blog`_
- `Planet Four Catalog`_, originally from https://www.planetfour.org/results
- The official Intake `examples`_

.. _Anaconda package data: https://github.com/ContinuumIO/anaconda-package-data
.. _this blog: https://www.anaconda.com/announcing-public-anaconda-package-download-data/
.. _Planet Four Catalog: https://github.com/michaelaye/p4catalog

Blogs
-----

These are Intake-related articles that may be of interest.

- `Discovering and Exploring Data in a Graphical Interface`_
- `Taking the Pain out of Data Access`_
- `Caching Data on First Read Makes Future Analysis Faster`_
- `Parsing Data from Filenames and Paths`_
- `Intake for cataloguing Spark`_
- `Intake released on Conda-Forge`_

.. _Discovering and Exploring Data in a Graphical Interface: https://www.anaconda.com/intake-discovering-and-exploring-data-in-a-graphical-interface/
.. _Intake for cataloguing Spark: https://www.anaconda.com/intake-for-cataloging-spark/
.. _Taking the Pain out of Data Access: https://www.anaconda.com/intake-taking-the-pain-out-of-data-access/
.. _Caching Data on First Read Makes Future Analysis Faster: https://www.anaconda.com/intake-caching-data-on-first-read-makes-future-analysis-faster/
.. _Parsing Data from Filenames and Paths: https://www.anaconda.com/intake-parsing-data-from-filenames-and-paths/
.. _Intake released on Conda-Forge: https://www.anaconda.com/intake-released-on-conda-forge/

Talks
-----

- `__init__ podcast interview (May 2019)`_
- `AnacondaCon (March 2019)`_
- `PyData DC (November 2018)`_
- `PyData NYC (October 2018)`_
- `ESIP tech dive (November 2018)`_

.. _\__init__ podcast interview (May 2019): https://www.pythonpodcast.com/intake-data-catalog-episode-213/
.. _ESIP tech dive (November 2018): https://www.youtube.com/watch?v=PSD7r3JFml0&feature=youtu.be
.. _PyData DC (November 2018): https://www.youtube.com/watch?v=OvZFtePHKXw
.. _PyData NYC (October 2018): https://www.youtube.com/watch?v=pjkMmJQfTb8
.. _AnacondaCon (March 2019): https://www.youtube.com/watch?v=oyZJrROQzUs

News
----

- See out `Wiki`_ page

.. _Wiki: https://github.com/intake/intake/wiki/Community-News
