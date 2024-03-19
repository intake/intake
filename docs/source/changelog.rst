Changelog
=========

2.0.4
-----

Released March 19, 2024

- re-enable v1 entrypoint sources
- expose recommend functions higher up (e.g,. intake.recommend)
- add more geo types and readers, including pmtiles
- add migration guide

2.0.3
-----

Released February 29, 2024

- fix v1 caches
- more docs

2.0.0
-----

Released Jan 31, 2024

- complete rewrite of the package, see main docs page

0.7.0
-----

Released May 29, 2023

- be able to override arguments when using a source defined in an entry-point
- make sources usable without explicit dependence on dask: zarr, textfiles, csv
- removed some explicit usage (but not all) of dask throughout the codebase
- new dataframe pipeline transform source

.. _v0.6.8:

0.6.8
-----

Released March 11, 2023

- user parameter parsed as string before conversion to given type
- numpy source becomes first to have read() path avoid dask
- when registering drivers dynamically, corresponding open_* functions
  will be created automatically (plus refactor/cleanup of the discovery code)
- docs config and style updates; the list of plugins to automatically
  pull in status badges
- catalog .gui attribute will make top-level GUI instance instead of
  cut down one-catalog version
- pre-commit checks added and consistent code style applied


.. _v0.6.7:

0.6.7
-----

Released February 13, 2023

- server fix for upstream dask change giving newlined in report
- editable plots, based on hvPlot's "explorer"
- remove "text" input to YAMLFileCatalog
- GUI bug fixes
- allow catalog TTL as None

.. _v0.6.6:

0.6.6
-----

Released on August 26, 2022.

- Fixed bug in json and jsonl driver.
- Ensure description is retained in the catalog.
- Fix cache issue when running inside a notebook.
- Add templating parameters.
- Plotting api keeps hold of hvplot calls to allow other plots to be made.
- docs updates
- fix urljoin for server via proxy

.. _v0.6.5:

0.6.5
-----

Released on January 9, 2022.

- Added link to intake-google-analytics.
- Add tiled driver.
- Add json and jsonl drivers.
- Allow parameters to be passed through catalog.
- Add mlist type which allows inputs from a known list of values.

.. raw:: html

    <script data-goatcounter="https://intake.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
