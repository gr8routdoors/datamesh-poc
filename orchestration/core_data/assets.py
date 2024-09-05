from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt.destinations import duckdb
from dlt_sources.sql_database import sql_database
import os
from .dlt_translator import CanonicalDagsterDltTranslator

# We wire this up manually so that we can support multiple sources of the same type
source_driver_name   = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
db_name              = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
schema_name          = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")
postgres_credentials = {
    "drivername": source_driver_name,
    "database":   db_name,
    "username":   os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME"),
    "password":   os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD"),
    "host":       os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST"),
    "port":       os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT"),
}

lake_name        = "lake"
lake_credentials = os.getenv("DESTINATION__LAKE__CREDENTIALS")
lake_destination = duckdb(credentials=lake_credentials)

@dlt_assets(
    dlt_source=sql_database(
        credentials=postgres_credentials,
        schema=schema_name,
    ),
    dlt_pipeline=pipeline(
        pipeline_name=f"{schema_name}_ingest",
        dataset_name=schema_name,
        destination=lake_destination,
        progress="log",
    ),
    name=schema_name,
    group_name="bronze",
    dagster_dlt_translator=CanonicalDagsterDltTranslator(source_driver_name, lake_name, db_name, schema_name)
)

def dlt_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
