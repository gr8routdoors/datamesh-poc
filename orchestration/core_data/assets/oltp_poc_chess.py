from collections.abc import Iterable
import os

from dagster import (
    AssetExecutionContext, 
    AssetKey, 
    AssetSpec, 
    SourceAsset, 
    multi_asset,
)
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
    constants,
)
from dlt import pipeline
from dlt.destinations import postgres

from dlt_sources.chess import source

# We wire this up manually so that we can support multiple sources of the same type
group_name              = "oltp"
db_name                 = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
schema_name             = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")

source_name             = "chess_api"
source_players_profiles = f"{source_name}__{db_name}__{schema_name}__players_profiles"
source_players_games    = f"{source_name}__{db_name}__{schema_name}__players_games"
dlt_source              = source(
    players=[
        "MagnusCarlsen",
        "Hikaru",
        "bogdandeac",
        "Prizant_academy",
        "lachesisq",
        # "HansOnTwitch",
        # "firouzja2003",
        # "fabianocaruana",
        # "nihalsarin",
        # "mishanick",
        # "mishanick",
        # "fairchess_on_youtube",
        # "baku_boulevard",
        # "anishgiri",
        # "vi_pranav",
        # "oleksandr_bortnyk",
    ],
    start_month="2024/01",
    end_month="2024/08",
).with_resources("players_profiles", "players_games")

destination_type             = "postgres"
destination_dataset          = schema_name
destination_name             = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
destination_username         = os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME")
destination_password         = os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD")
destination_host             = os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST")
destination_port             = os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT")
destination_credentials      = f"{destination_name}://{destination_username}:{destination_password}@{destination_host}:{destination_port}/{db_name}"
destination                  = postgres(credentials=destination_credentials)
destination_players_profiles = f"{destination_name}__{db_name}__{schema_name}__players_profiles"
destination_players_games    = f"{destination_name}__{db_name}__{schema_name}__players_games"

dlt_pipeline           = pipeline(
    pipeline_name=f"${db_name}__{schema_name}__{group_name}",
    dataset_name=destination_dataset,
    destination=destination,
    progress="log",
)

class ApiDagsterDltTranslator(DagsterDltTranslator):
    """Allows for setting custom prefixes for DLT assets with API sources"""

    def __init__(self, source_name, destination_name, database_name, schema_name):
        self.source_name = source_name
        self.destination_name = destination_name
        self.database_name = database_name
        self.schema_name = schema_name    
    
    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey(f"{self.destination_name}__{self.database_name}__{self.schema_name}__{resource.name}")

    def get_deps_asset_keys(self, resource: DagsterDltResource) -> Iterable[AssetKey]:
        """Overrides upstream asset key to be a single source asset."""
        return [AssetKey(f"{self.source_name}__{resource.name}")]

dagster_dlt_translator = ApiDagsterDltTranslator(source_name, destination_name, db_name, schema_name)

asset_metadata = {
    constants.META_KEY_SOURCE: dlt_source,
    constants.META_KEY_PIPELINE: dlt_pipeline,
    constants.META_KEY_TRANSLATOR: dagster_dlt_translator,
}
asset_tags     = {
    "dagster/storage_kind": destination_type,
}


# We use multi_asset instead of dlt_assets because of issues chaining DLT pipelines in dagster (duplicate AssetKey error for upstream deps)
@multi_asset(
    name=f"{db_name}__{schema_name}__{group_name}",
    group_name=group_name,
    compute_kind="dlt",
    can_subset=True,
    specs=[
        AssetSpec(
            key=destination_players_profiles,
            skippable=True,
            deps=[source_players_profiles],
            metadata={
                "storage_table":f"{db_name}.{schema_name}.players_profiles",
                **asset_metadata,
            },
            tags=asset_tags,
        ),
        AssetSpec(
            key=destination_players_games, 
            skippable=True, 
            deps=[source_players_games],
            metadata={
                "storage_table":f"{db_name}.{schema_name}.players_games",
                **asset_metadata,
            },
            tags=asset_tags,
        ),
    ],
)
def dlt_asset_factory(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

# Correct the group name from `default` for upstream dependencies
assets_chess_sources     = [
    SourceAsset(key, group_name=source_name) for key in dlt_asset_factory.dependency_keys
]
