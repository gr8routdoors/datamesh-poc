from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt.destinations import duckdb
from dlt_sources.sql_database import sql_database
import os
from .dlt_translator import PrefixDagsterDltTranslator

# We wire this up manually so that we can support multiple sources of the same type
postgres_credentials = {
    "drivername": os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME"),
    "database":   os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE"),
    "username":   os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME"),
    "password":   os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD"),
    "host":       os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST"),
    "port":       os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT"),
}
schema_name = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")

datalake_credentials = os.getenv("DESTINATION__DATALAKE__CREDENTIALS")
datalake_destination = duckdb(credentials=datalake_credentials)

@dlt_assets(
    dlt_source=sql_database(
        credentials=postgres_credentials,
        schema=schema_name,
    ),
    dlt_pipeline=pipeline(
        pipeline_name=f"{schema_name}_ingest",
        dataset_name=schema_name,
        destination=datalake_destination,
        progress="log",
    ),
    name=schema_name,
    group_name="bronze",
    dagster_dlt_translator=PrefixDagsterDltTranslator("datalake", f"{schema_name}_db")
)

def dlt_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
