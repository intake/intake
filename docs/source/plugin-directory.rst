.. _plugin-directory:

Plugin Directory
================

This is a list of known projects which install driver plugins for Intake, and the named drivers each
contains in parentheses:

* builtin to Intake (``catalog``, ``csv``, ``intake_remote``, ``ndzarr``,
  ``numpy``, ``textfiles``, ``yaml_file_cat``, ``yaml_files_cat``)
* `intake-astro <https://github.com/intake/intake-astro>`_ Table and array loading of FITS astronomical data (``fits_array``, ``fits_table``)
* `intake-accumulo <https://github.com/intake/intake-accumulo>`_ Apache Accumulo clustered data storage (``accumulo``)
* `intake-avro <https://github.com/intake/intake-avro>`_: Apache Avro data serialization format (``avro_table``, ``avro_sequence``
* `intake-cmip <https://github.com/NCAR/intake-cmip>`_:  load `CMIP <https://cmip.llnl.gov/>`_ (Coupled Model Intercomparison Project) data (``cmip5``)
* `intake-bluesky <https://nsls-ii.github.io/intake-bluesky/>`_: search and retrieve data in the `bluesky <https://nsls-ii.github.io/bluesky>`_ data model
* `intake-dynamodb <https://github.com/informatics-lab/intake-dynamodb>`_ link to Amazon DynamoDB (``dynamodb``)
* `intake-elasticsearch <https://github.com/intake/intake-elasticsearch>`_: Elasticsearch search and analytics engine (``elasticsearch_seq``, ``elasticsearch_table``)
* `intake-geopandas <https://github.com/informatics-lab/intake_geopandas>`_: load ESRI Shape Files with geopandas (``shape``)
* `intake-hbase <https://github.com/intake/intake-hbase>`_: Apache HBase database (``hbase``)
* `intake-iris <https://github.com/informatics-lab/intake-iris>`_ load netCDF and GRIB files with IRIS (``grib``, ``netcdf``)
* `intake-mongo <https://github.com/intake/intake-mongo>`_: MongoDB noSQL query (``mongo``)
* `intake-netflow <https://github.com/intake/intake-netflow>`_: Netflow packet format (``netflow``)
* `intake-odbc <https://github.com/intake/intake-odbc>`_: ODBC database (``odbc``)
* `intake-parquet <https://github.com/intake/intake-parquet>`_: Apache Parquet file format (``parquet``)
* `intake-pcap <https://github.com/intake/intake-pcap>`_: PCAP network packet format (``pcap``)
* `intake-postgres <https://github.com/intake/intake-postgres>`_: PostgreSQL database (``postgres``)
* `intake-s3-manifests <https://github.com/informatics-lab/intake-s3-manifests>`_ (``s3_manifest``)
* `intake-solr <https://github.com/intake/intake-solr>`_: Apache Solr search platform (``solr``)
* `intake-spark <https://github.com/intake/intake-spark>`_: data processed by Apache Spark (``spark_cat``, ``spark_rdd``, ``spark_dataframe``)
* `intake-sql <https://github.com/intake/intake-sql>`_: Generic SQL queries via SQLAlchemy (``sql_cat``, ``sql``, ``sql_auto``, ``sql_manual``)
* `intake-splunk <https://github.com/intake/intake-splunk>`_: Splunk machine data query (``splunk``)
* `intake-xarray <https://github.com/intake/intake-xarray>`_: load netCDF, Zarr and other multi-dimensional data (``xarray_image``, ``netcdf``, ``opendap``,
  ``rasterio``, ``remote-xarray``, ``zarr``)

The status of these projects is available at `Status Dashboard <https://intake.github.io/status/>`_.

Don't see your favorite format?  See :doc:`making-plugins` for how to create new plugins.

Note that if you want your plugin listed here, open an issue in the `Intake
issue repository <https://github.com/intake/intake>`_ and add an entry to the
`status dashboard repository <https://github.com/intake/intake-dashboard>`_. We also have a
`plugin wishlist Github issue <https://github.com/intake/intake/issues/58>`_
that shows the breadth of plugins we hope to see for Intake.
