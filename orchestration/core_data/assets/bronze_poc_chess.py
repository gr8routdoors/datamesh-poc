# Example of our OLTP database (postgres) ingest into a (duckdb) data lake

import os

from dagster import AssetExecutionContext, AssetSpec, SourceAsset, multi_asset
from dagster_embedded_elt.dlt import DagsterDltResource, constants
from dlt import pipeline
from dlt.destinations import duckdb

from dlt_sources.sql_database import sql_database
from dlt_sources.dbt_dlt_translator import CanonicalDagsterDltTranslator

# We wire this up manually so that we can support multiple sources of the same type
source_group_name    = "oltp"
group_name           = "bronze"
db_name              = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
schema_name          = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")

source_name             = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
source_credentials      = {
    "drivername": source_name,
    "database":   db_name,
    "username":   os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME"),
    "password":   os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD"),
    "host":       os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST"),
    "port":       os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT"),
}
source_players_profiles = f"{source_name}__{db_name}__{schema_name}__players_profiles"
source_players_games    = f"{source_name}__{db_name}__{schema_name}__players_games"
dlt_source              = sql_database(
    credentials=source_credentials,
    schema=schema_name,
    table_names=["players_profiles", "players_games"]
)

destination_name             = "lake"
destination_type             = "duckdb"
destination_credentials      = os.getenv("DESTINATION__LAKE__CREDENTIALS")
destination                  = duckdb(credentials=destination_credentials)
destination_players_profiles = f"{destination_name}__{db_name}__{schema_name}__players_profiles"
destination_players_games    = f"{destination_name}__{db_name}__{schema_name}__players_games"

dlt_pipeline           = pipeline(
    pipeline_name=f"{db_name}__{schema_name}__{group_name}",
    dataset_name=schema_name,
    destination=destination,
    progress="log",
)
dagster_dlt_translator = CanonicalDagsterDltTranslator(source_name, destination_name, db_name, schema_name)


# @dlt_assets(
#     dlt_source=sql_database(
#         credentials=source_credentials,
#         schema=schema_name,
#         table_names=["players_profiles", "players_games"]
#     ),
#     dlt_pipeline=pipeline(
#         pipeline_name=f"{db_name}__{schema_name}__{group_name}",
#         dataset_name=schema_name,
#         destination=destination,
#         progress="log",
#     ),
#     name=f"{db_name}__{schema_name}__{group_name}",
#     group_name=group_name,
#     dagster_dlt_translator=CanonicalDagsterDltTranslator(source_name, destination_name, db_name, schema_name)
# )

asset_metadata = {
    constants.META_KEY_SOURCE: dlt_source,
    constants.META_KEY_PIPELINE: dlt_pipeline,
    constants.META_KEY_TRANSLATOR: dagster_dlt_translator,
}
asset_tags     = {
    "dagster/storage_kind": destination_type,
}

# We use multi_asset instead of dlt_assets because, as of this writing, dagster doesn't support chaining DLT pipelines (duplicate AssetKey error for upstream deps)
@multi_asset(
    name=f"{db_name}__{schema_name}__{group_name}",
    group_name=group_name,
    compute_kind="duckdb",
    can_subset=True,
    specs=[
        AssetSpec(
            key=destination_players_profiles,
            skippable=True,
            deps=[source_players_profiles],
            metadata=asset_metadata,
            tags=asset_tags,
        ),
        AssetSpec(
            key=destination_players_games, 
            skippable=True, 
            deps=[source_players_games],
            metadata=asset_metadata,
            tags=asset_tags,
        ),
    ],
)
def dlt_asset_factory(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
