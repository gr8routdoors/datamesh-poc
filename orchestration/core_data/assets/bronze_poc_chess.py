import os

from dagster import asset
from dagster_duckdb import DuckDBResource

from dlt_sources.sql_database import sql_database
from dlt_sources.dbt_dlt_translator import CanonicalDagsterDltTranslator


source_name          = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
db_name              = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
schema_name          = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")
lake_schema_name     = "bronze"

# The dependencies for these assets come from DLT in oltp_poc_chess.py

players_profiles = f"{source_name}__{db_name}__{schema_name}__players_profiles"
@asset(
    deps       = [players_profiles],
    group_name = lake_schema_name,
)
def bronze__poc__chess__players_profiles(duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute(
            f"CREATE TABLE {lake_schema_name}.{db_name}__{schema_name}__players_profiles AS SELECT * FROM {players_profiles}"
        )


players_games = f"{source_name}__{db_name}__{schema_name}__players_games"
@asset(
    deps       = [players_games],
    group_name = lake_schema_name,
)
def bronze__poc__chess__players_games(duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute(
            f"CREATE TABLE {lake_schema_name}.{db_name}__{schema_name}__players_games AS SELECT * FROM {players_games}"
        )


players_online_status = f"{source_name}__{db_name}__{schema_name}__players_online_status"
@asset(
    deps       = [players_online_status],
    group_name = lake_schema_name,
)
def bronze__poc__chess__players_online_status(duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute(
            f"CREATE TABLE {lake_schema_name}.{db_name}__{schema_name}__players_online_status AS SELECT * FROM {players_online_status}"
        )
