import os

from dagster import AssetExecutionContext, AssetSpec, SourceAsset, multi_asset
from dagster_embedded_elt.dlt import constants, DagsterDltResource
from dlt import pipeline
from dlt.destinations import postgres

from dlt_sources.chess import source
from dlt_sources.dbt_dlt_translator import ApiDagsterDltTranslator

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
    dataset_name=schema_name,
    destination=destination,
    progress="log",
)
dagster_dlt_translator = ApiDagsterDltTranslator(source_name, destination_name, db_name, schema_name)

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
    compute_kind=destination_type,
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

# Correct the group name from `default` for upstream dependencies
assets_chess_sources     = [
    SourceAsset(key, group_name=source_name) for key in dlt_asset_factory.dependency_keys
]
