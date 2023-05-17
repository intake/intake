class FileData:
    ...


class Service:
    ...


class Catalog:
    ...


class Parquet(FileData):
    extensions = {"parq", "parquet", "/"}
    mimetypes = {"application/vnd.apache.parquet"}
    structure = {"table", "nested"}


class CSV(FileData):
    extensions = {"csv", "txt", "tsv"}
    mimetypes = {"text/csv", "application/csv", "application/vnd.ms-excel"}
    structure = {"table"}


class Text(FileData):
    extensions = {"txt", "text"}
    mimetypes = {"text/*"}
    structure = {"sequence"}


class SQLQuery(Service):
    structure = {"sequence", "table"}


class CatalogFile(Catalog, FileData):
    extensions = {"yaml", "yml"}
    mimetypes = {"text/yaml"}


class YAMLFile(FileData):
    extensions = {"yaml", "yml"}
    mimetypes = {"text/yaml"}
    structure = {"nested"}


class JSONFile(FileData):
    # could be any API

    extensions = {"json"}
    mimetypes = {"text/json", "application/json"}
    structure = {"nested", "table"}
