# Test to feed the lake from postgres

import os

from dlt import pipeline
from dlt.destinations import duckdb

from sql_database import sql_database


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

dlt_source = sql_database(
    credentials=source_credentials,
    schema=schema_name,
    table_names=["players_profiles", "players_games"]
)

dlt_pipeline = pipeline(
    pipeline_name=f"{db_name}__{schema_name}__{group_name}",
    dataset_name=schema_name,
    destination=destination,
    progress="log",
)

result = dlt_pipeline.run(dlt_source.with_resources("players_profiles", "players_games"))
print(result)
