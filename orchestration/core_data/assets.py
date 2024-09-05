from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt.destinations import duckdb
from dlt_sources.sql_database import sql_database
import os

postgres_credentials = {
    "drivername": os.getenv("SOURCES__SQL_DATABASE__CREDENTAILS__DRIVERNAME"),
    "database":   os.getenv("SOURCES__SQL_DATABASE__CREDENTAILS__DATABASE"),
    "username":   os.getenv("SOURCES__SQL_DATABASE__CREDENTAILS__USERNAME"),
    "password":   os.getenv("SOURCES__SQL_DATABASE__CREDENTAILS__PASSWORD"),
    "host":       os.getenv("SOURCES__SQL_DATABASE__CREDENTAILS__HOST"),
    "port":       os.getenv("SOURCES__SQL_DATABASE__CREDENTAILS__PORT"),
    "drivername": os.getenv("SOURCES__SQL_DATABASE__CREDENTAILS__DRIVERNAME"),
}

duckdb_credentials = os.getenv("DESTINATION__DUCKDB__CREDENTIALS")
duckdb_destination = duckdb(credentials=duckdb_credentials)

@dlt_assets(
    dlt_source=sql_database(
        credentials=postgres_credentials,
        schema="ecom",
    ),
    dlt_pipeline=pipeline(
        pipeline_name="ecom_ingest",
        dataset_name="ecom",
        destination=duckdb_destination,
        progress="log",
    ),
    name="ecom",
    group_name="bronze",
)
def dlt_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
