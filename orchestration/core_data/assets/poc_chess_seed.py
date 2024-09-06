import os

from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt.destinations import postgres

from dlt_sources.chess import source
from dlt_sources.dbt_dlt_translator import CanonicalDagsterDltTranslator

# We wire this up manually so that we can support multiple sources of the same type
group_name              = "oltp"
db_name                 = os.getenv("SOURCES__POC_DB__CREDENTIALS__DATABASE")
schema_name             = os.getenv("SOURCES__POC_DB__CREDENTIALS__SCHEMA")

source_name             = "chess"

destination_name        = os.getenv("SOURCES__POC_DB__CREDENTIALS__DRIVERNAME")
destination_credentials = postgres_credentials = {
    "drivername": destination_name,
    "database":   db_name,
    "username":   os.getenv("SOURCES__POC_DB__CREDENTIALS__USERNAME"),
    "password":   os.getenv("SOURCES__POC_DB__CREDENTIALS__PASSWORD"),
    "host":       os.getenv("SOURCES__POC_DB__CREDENTIALS__HOST"),
    "port":       os.getenv("SOURCES__POC_DB__CREDENTIALS__PORT"),
}
destination             = postgres(credentials=destination_credentials)

@dlt_assets(
    dlt_source=source(
        players=[
            "MagnusCarlsen",
            "Hikaru",
            "DogdanDeac",
            "Prizant_academy",
            "lachesisq"
            "HansOnTwitch",
            "firouzja2003",
            "fabianocaruana",
            "nihalsarin",
            "mishanick",
            "mishanick",
            "fairchess_on_youtube",
            "baku_boulevard",
            "anishgiri",
            "vi_pranav",
            "oleksandr_bortnyk",
        ]
    ),
    dlt_pipeline=pipeline(
        pipeline_name=f"${db_name}__{schema_name}__{group_name}",
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
