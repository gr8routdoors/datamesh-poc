# Example of our OLTP database (postgres) ingest into a (duckdb) data lake

from collections.abc import Iterable
import os

from dagster import AssetExecutionContext, AssetKey, AssetSpec, multi_asset
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
    constants,
)
from dlt import pipeline
from dlt.destinations import duckdb

from dlt_sources.sql_database import sql_database

# We wire this up manually so that we can support multiple sources of the same type
source_group_name       = "oltp"
source_name             = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
source_database         = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
source_schema           = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")
source_credentials      = {
    "drivername": source_name,
    "database":   source_database,
    "username":   os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME"),
    "password":   os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD"),
    "host":       os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST"),
    "port":       os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT"),
}
dlt_source              = sql_database(
    credentials=source_credentials,
    schema=source_schema,
    table_names=["players_profiles", "players_games"]
)

destination_group_name       = "raw"
destination_name             = "duckdb"
destination_database         = "lake"
destination_schema           = "raw"
destination_credentials      = os.getenv("DESTINATION__LAKE__CREDENTIALS")
dlt_destination              = duckdb(credentials=destination_credentials)

pipeline_name = f"{destination_name}__{destination_database}__{destination_schema}__{source_schema}"
dlt_pipeline  = pipeline(
    pipeline_name=pipeline_name,
    dataset_name=destination_schema,
    destination=dlt_destination,
    progress="log",
)

class CanonicalDagsterDltTranslator(DagsterDltTranslator):
    """Allows for setting custom prefixes for DLT assets"""

    def __init__(self, source_name, source_database, source_schema, destination_schema):
        self.source_name          = source_name
        self.source_database      = source_database
        self.source_schema        = source_schema
        self.destination_database = destination_database
        self.destination_schema   = destination_schema  
    
    def get_destination_asset_name(self, resource_name: str) -> str:
        return f"{self.destination_schema}__{resource_name}"
    
    def get_source_asset_name(self, resource_name: str) -> str:
        return f"{self.source_name}__{self.source_database}__{self.source_schema}__{resource_name}"

    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey(self.get_destination_asset_name(resource.name))

    def get_deps_asset_keys(self, resource: DagsterDltResource) -> Iterable[AssetKey]:
        """Overrides upstream asset key to be a single source asset."""
        return [AssetKey(self.get_source_asset_name(resource))]

dagster_dlt_translator       = CanonicalDagsterDltTranslator(source_name, source_database, source_schema, destination_schema)
source_players_profiles      = dagster_dlt_translator.get_source_asset_name("players_profiles")
source_players_games         = dagster_dlt_translator.get_source_asset_name("players_games")
destination_players_profiles = dagster_dlt_translator.get_destination_asset_name("players_profiles")
destination_players_games    = dagster_dlt_translator.get_destination_asset_name("players_games")

asset_metadata = {
    constants.META_KEY_SOURCE: dlt_source,
    constants.META_KEY_PIPELINE: dlt_pipeline,
    constants.META_KEY_TRANSLATOR: dagster_dlt_translator,
}
asset_tags     = {
    "dagster/storage_kind": destination_name,
}

# We use multi_asset instead of dlt_assets because of issues chaining DLT pipelines in dagster (duplicate AssetKey error for upstream deps)
@multi_asset(
    name=pipeline_name,
    group_name=destination_group_name,
    compute_kind="dlt",
    can_subset=True,
    specs=[
        AssetSpec(
            key=destination_players_profiles,
            skippable=True,
            deps=[source_players_profiles],
            metadata={
                "storage_table":f"{destination_schema}.players_profiles",
                **asset_metadata,
            },
            tags=asset_tags,
        ),
        AssetSpec(
            key=destination_players_games, 
            skippable=True, 
            deps=[source_players_games],
            metadata={
                "storage_table":f"{destination_schema}.players_games",
                **asset_metadata,
            },
            tags=asset_tags,
        ),
    ],
)
def dlt_asset_factory(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
