import os

from dagster import AssetExecutionContext, SourceAsset
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt.destinations import duckdb

from dlt_sources.sql_database import sql_database
from dlt_sources.dbt_dlt_translator import CanonicalDagsterDltTranslator

# We wire this up manually so that we can support multiple sources of the same type
source_group_name    = "oltp"
group_name           = "bronze"
db_name              = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
schema_name          = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")

source_name          = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
source_credentials   = {
    "drivername": source_name,
    "database":   db_name,
    "username":   os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME"),
    "password":   os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD"),
    "host":       os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST"),
    "port":       os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT"),
}

destination_name        = "lake"
destination_credentials = os.getenv("DESTINATION__LAKE__CREDENTIALS")
destination             = duckdb(credentials=destination_credentials)

@dlt_assets(
    dlt_source=sql_database(
        credentials=source_credentials,
        schema=schema_name,
    ),
    dlt_pipeline=pipeline(
        pipeline_name=f"{db_name}__{schema_name}__{group_name}",
        dataset_name=schema_name,
        destination=destination,
        progress="log",
    ),
    name=f"{db_name}__{schema_name}__{group_name}",
    group_name=group_name,
    dagster_dlt_translator=CanonicalDagsterDltTranslator(source_name, destination_name, db_name, schema_name)
)

def dlt_asset_factory(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

chess_lake_assets = [
    SourceAsset(key, group_name=source_group_name) for key in dlt_asset_factory.dependency_keys
]
