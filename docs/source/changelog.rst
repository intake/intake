Changelog
=========

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
