
.. _api2:

API Reference
=============

User Functions
--------------

.. autosummary::
    intake.config.Config
    intake.readers.datatypes.recommend
    intake.readers.convert.auto_pipeline
    intake.readers.convert.path
    intake.readers.entry.Catalog
    intake.readers.entry.DataDescription
    intake.readers.entry.ReaderDescription
    intake.readers.readers.recommend
    intake.readers.readers.reader_from_call

.. autoclass:: intake.config.Config
    :members:

.. autofunction:: intake.readers.datatypes.recommend

.. autofunction:: intake.readers.convert.auto_pipeline

.. autoclass:: intake.readers.entry.Catalog
    :members:

.. autoclass:: intake.readers.entry.DataDescription
    :members:

.. autoclass:: intake.readers.entry.ReaderDescription
    :members:

.. autofunction:: intake.readers.readers.recommend

.. autofunction:: intake.readers.readers.reader_from_call

.. _base:

Base Classes
------------

These may be subclassed by developers

.. autosummary::
   intake.readers.datatypes.BaseData
   intake.readers.readers.BaseReader
   intake.readers.convert.BaseConverter
   intake.readers.namespaces.Namespace
   intake.readers.search.SearchBase
   intake.readers.user_parameters.BaseUserParameter

.. autoclass:: intake.readers.datatypes.BaseData
   :members:

.. autoclass:: intake.readers.readers.BaseReader
   :members:

.. autoclass:: intake.readers.convert.BaseConverter
   :members:

.. autoclass:: intake.readers.namespaces.Namespace
   :members:

.. autoclass:: intake.readers.search.SearchBase
   :members:

.. autoclass:: intake.readers.user_parameters.BaseUserParameter
   :members:

.. _data:

Data Classes
------------

.. autosummary::
   intake.readers.datatypes.ASDF
   intake.readers.datatypes.AVRO
   intake.readers.datatypes.CSV
   intake.readers.datatypes.Catalog
   intake.readers.datatypes.CatalogAPI
   intake.readers.datatypes.CatalogFile
   intake.readers.datatypes.DICOM
   intake.readers.datatypes.DeltalakeTable
   intake.readers.datatypes.Excel
   intake.readers.datatypes.FITS
   intake.readers.datatypes.Feather1
   intake.readers.datatypes.Feather2
   intake.readers.datatypes.FileData
   intake.readers.datatypes.GDALRasterFile
   intake.readers.datatypes.GDALVectorFile
   intake.readers.datatypes.GRIB2
   intake.readers.datatypes.GeoJSON
   intake.readers.datatypes.GeoPackage
   intake.readers.datatypes.HDF5
   intake.readers.datatypes.Handle
   intake.readers.datatypes.HuggingfaceDataset
   intake.readers.datatypes.IcebergDataset
   intake.readers.datatypes.JPEG
   intake.readers.datatypes.JSONFile
   intake.readers.datatypes.KerasModel
   intake.readers.datatypes.Literal
   intake.readers.datatypes.MatlabArray
   intake.readers.datatypes.MatrixMarket
   intake.readers.datatypes.NetCDF3
   intake.readers.datatypes.Nifti
   intake.readers.datatypes.NumpyFile
   intake.readers.datatypes.ORC
   intake.readers.datatypes.OpenDAP
   intake.readers.datatypes.PNG
   intake.readers.datatypes.Parquet
   intake.readers.datatypes.PickleFile
   intake.readers.datatypes.Prometheus
   intake.readers.datatypes.PythonSourceCode
   intake.readers.datatypes.RawBuffer
   intake.readers.datatypes.SKLearnPickleModel
   intake.readers.datatypes.SQLQuery
   intake.readers.datatypes.SQLite
   intake.readers.datatypes.STACJSON
   intake.readers.datatypes.Service
   intake.readers.datatypes.Shapefile
   intake.readers.datatypes.TFRecord
   intake.readers.datatypes.THREDDSCatalog
   intake.readers.datatypes.TIFF
   intake.readers.datatypes.Text
   intake.readers.datatypes.TileDB
   intake.readers.datatypes.TiledDataset
   intake.readers.datatypes.TiledService
   intake.readers.datatypes.WAV
   intake.readers.datatypes.XML
   intake.readers.datatypes.YAMLFile
   intake.readers.datatypes.Zarr

.. _reader:

Reader Classes
--------------

Includes readers, transformers, converters and output classes.

.. autosummary::
   intake.readers.catalogs.EarthdataCatalogReader
   intake.readers.catalogs.EarthdataReader
   intake.readers.catalogs.HuggingfaceHubCatalog
   intake.readers.catalogs.SKLearnExamplesCatalog
   intake.readers.catalogs.SQLAlchemyCatalog
   intake.readers.catalogs.STACIndex
   intake.readers.catalogs.StacCatalogReader
   intake.readers.catalogs.StacSearch
   intake.readers.catalogs.StackBands
   intake.readers.catalogs.THREDDSCatalogReader
   intake.readers.catalogs.TensorFlowDatasetsCatalog
   intake.readers.catalogs.TiledCatalogReader
   intake.readers.catalogs.TorchDatasetsCatalog
   intake.readers.convert.ASDFToNumpy
   intake.readers.convert.BaseConverter
   intake.readers.convert.DaskArrayToTileDB
   intake.readers.convert.DaskDFToPandas
   intake.readers.convert.DaskToRay
   intake.readers.convert.DeltaQueryToDask
   intake.readers.convert.DeltaQueryToDaskGeopandas
   intake.readers.convert.DicomToNumpy
   intake.readers.convert.DuckToPandas
   intake.readers.convert.FITSToNumpy
   intake.readers.convert.GenericFunc
   intake.readers.convert.HuggingfaceToRay
   intake.readers.convert.NibabelToNumpy
   intake.readers.convert.NumpyToTileDB
   intake.readers.convert.PandasToGeopandas
   intake.readers.convert.PandasToMetagraph
   intake.readers.convert.PandasToPolars
   intake.readers.convert.PandasToRay
   intake.readers.convert.Pipeline
   intake.readers.convert.PolarsEager
   intake.readers.convert.PolarsLazy
   intake.readers.convert.PolarsToPandas
   intake.readers.convert.RayToDask
   intake.readers.convert.RayToPandas
   intake.readers.convert.RayToSpark
   intake.readers.convert.SparkDFToRay
   intake.readers.convert.TileDBToNumpy
   intake.readers.convert.TileDBToPandas
   intake.readers.convert.TiledNodeToCatalog
   intake.readers.convert.TiledSearch
   intake.readers.convert.ToHvPlot
   intake.readers.convert.TorchToRay
   intake.readers.output.CatalogToJson
   intake.readers.output.DaskArrayToZarr
   intake.readers.output.GeopandasToFile
   intake.readers.output.MatplotlibToPNG
   intake.readers.output.NumpyToNumpyFile
   intake.readers.output.PandasToCSV
   intake.readers.output.PandasToFeather
   intake.readers.output.PandasToHDF5
   intake.readers.output.PandasToParquet
   intake.readers.output.Repr
   intake.readers.output.ToMatplotlib
   intake.readers.output.XarrayToNetCDF
   intake.readers.output.XarrayToZarr
   intake.readers.readers.ASDFReader
   intake.readers.readers.Awkward
   intake.readers.readers.AwkwardAVRO
   intake.readers.readers.AwkwardJSON
   intake.readers.readers.AwkwardParquet
   intake.readers.readers.Condition
   intake.readers.readers.CupyNumpyReader
   intake.readers.readers.CupyTextReader
   intake.readers.readers.DaskAwkwardJSON
   intake.readers.readers.DaskAwkwardParquet
   intake.readers.readers.DaskCSV
   intake.readers.readers.DaskDF
   intake.readers.readers.DaskDeltaLake
   intake.readers.readers.DaskHDF
   intake.readers.readers.DaskJSON
   intake.readers.readers.DaskNPYStack
   intake.readers.readers.DaskParquet
   intake.readers.readers.DaskSQL
   intake.readers.readers.DaskZarr
   intake.readers.readers.DeltaReader
   intake.readers.readers.DicomReader
   intake.readers.readers.DuckCSV
   intake.readers.readers.DuckDB
   intake.readers.readers.DuckJSON
   intake.readers.readers.DuckParquet
   intake.readers.readers.DuckSQL
   intake.readers.readers.FITSReader
   intake.readers.readers.FileByteReader
   intake.readers.readers.FileExistsReader
   intake.readers.readers.FileReader
   intake.readers.readers.GeoPandasReader
   intake.readers.readers.GeoPandasTabular
   intake.readers.readers.HandleToUrlReader
   intake.readers.readers.HuggingfaceReader
   intake.readers.readers.KerasAudio
   intake.readers.readers.KerasImageReader
   intake.readers.readers.KerasModelReader
   intake.readers.readers.KerasText
   intake.readers.readers.NibabelNiftiReader
   intake.readers.readers.NumpyReader
   intake.readers.readers.NumpyText
   intake.readers.readers.NumpyZarr
   intake.readers.readers.Pandas
   intake.readers.readers.PandasCSV
   intake.readers.readers.PandasExcel
   intake.readers.readers.PandasFeather
   intake.readers.readers.PandasHDF5
   intake.readers.readers.PandasORC
   intake.readers.readers.PandasParquet
   intake.readers.readers.PandasSQLAlchemy
   intake.readers.readers.Polars
   intake.readers.readers.PolarsAvro
   intake.readers.readers.PolarsCSV
   intake.readers.readers.PolarsDeltaLake
   intake.readers.readers.PolarsExcel
   intake.readers.readers.PolarsFeather
   intake.readers.readers.PolarsIceberg
   intake.readers.readers.PolarsJSON
   intake.readers.readers.PolarsParquet
   intake.readers.readers.PrometheusMetricReader
   intake.readers.readers.PythonModule
   intake.readers.readers.RasterIOXarrayReader
   intake.readers.readers.Ray
   intake.readers.readers.RayBinary
   intake.readers.readers.RayCSV
   intake.readers.readers.RayDeltaLake
   intake.readers.readers.RayJSON
   intake.readers.readers.RayParquet
   intake.readers.readers.RayText
   intake.readers.readers.Retry
   intake.readers.readers.SKImageReader
   intake.readers.readers.SKLearnExampleReader
   intake.readers.readers.SKLearnModelReader
   intake.readers.readers.ScipyMatlabReader
   intake.readers.readers.ScipyMatrixMarketReader
   intake.readers.readers.SparkCSV
   intake.readers.readers.SparkDataFrame
   intake.readers.readers.SparkDeltaLake
   intake.readers.readers.SparkParquet
   intake.readers.readers.SparkText
   intake.readers.readers.TFORC
   intake.readers.readers.TFPublicDataset
   intake.readers.readers.TFRecordReader
   intake.readers.readers.TFSQL
   intake.readers.readers.TFTextreader
   intake.readers.readers.TileDBDaskReader
   intake.readers.readers.TileDBReader
   intake.readers.readers.TiledClient
   intake.readers.readers.TiledNode
   intake.readers.readers.TorchDataset
   intake.readers.readers.XArrayDatasetReader
   intake.readers.readers.YAMLCatalogReader
   intake.readers.transform.DataFrameColumns
   intake.readers.transform.GetItem
   intake.readers.transform.Method
   intake.readers.transform.PysparkColumns
   intake.readers.transform.THREDDSCatToMergedDataset
   intake.readers.transform.XarraySel
