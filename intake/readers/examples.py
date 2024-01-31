"""This module can contain examples of complex Intake use we wish to refer to"""
import operator


def ms_building_parquet():
    """22k build polygon outlines in the US Virgin Islands"""
    import intake.readers
    import planetary_computer

    cat = intake.readers.datatypes.STACJSON(
        "https://planetarycomputer.microsoft.com/api/stac/v1"
    ).to_reader(
        reader="StacSearch",
        query={"collections": ["ms-buildings"]},
        signer=planetary_computer.sign_inplace,
        prefer="Awkward",
    )
    return cat.apply(operator.getitem, "USVirginIslands_32300213_2023-04-25").apply(
        operator.getitem,
        "data",
        output_instance="intake.readers.readers:AwkwardParquet",
    )


ms_us_virgin_cat = """
aliases:
  building_outlines: 1e150595b4343b5a
data:
  ea539c01591e5e69:
    datatype: intake.readers.datatypes:STACJSON
    kwargs:
      metadata: {}
      storage_options: null
      url: https://planetarycomputer.microsoft.com/api/stac/v1
    metadata: {}
    user_parameters: {}
entries:
  1e150595b4343b5a:
    kwargs:
      out_instances:
      - intake.readers.entry:Catalog
      - intake.readers.entry:Catalog
      - intake.readers.entry:Catalog
      steps:
      - - '{data(bd3879bda378781f)}'
        - []
        - {}
      - - '{func(intake.readers.convert:GenericFunc)}'
        - - USVirginIslands_32300213_2023-04-25
        - func: '{func(_operator:getitem)}'
      - - '{func(intake.readers.convert:GenericFunc)}'
        - - data
        - func: '{func(_operator:getitem)}'
    metadata: {}
    output_instance: intake.readers.entry:Catalog
    reader: intake.readers.convert:Pipeline
    user_parameters: {}
  bd3879bda378781f:
    kwargs:
      data: '{data(ea539c01591e5e69)}'
      prefer: Awkward
      query:
        collections:
        - ms-buildings
      signer: '{func(planetary_computer.sas:sign_inplace)}'
    metadata: {}
    output_instance: intake.readers.entry:Catalog
    reader: intake.readers.catalogs:StacSearch
    user_parameters: {}
metadata: {}
user_parameters: {}
version: 2
"""


def ms_delta_buildings():
    # replicates https://planetarycomputer.microsoft.com/dataset/ms-buildings#Example-Notebook
    import intake.readers
    import planetary_computer

    cat = intake.readers.datatypes.STACJSON(
        "https://planetarycomputer.microsoft.com/api/stac/v1"
    ).to_reader(reader="StacCatalog", signer=planetary_computer.sign_inplace, prefer="Delta")
    return cat.apply(operator.getitem, "ms-buildings").apply(
        operator.getitem, "delta", output_instance="deltalake:DeltaTable"
    )
