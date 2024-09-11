import os
from dagster import Definitions, load_assets_from_modules
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource

from .assets import bronze_poc_chess

defs = Definitions(
    assets    = load_assets_from_modules([bronze_poc_chess]),
    resources = {
        "dlt": DagsterDltResource(),
        "duckdb": DuckDBResource(database=os.getenv("DESTINATION__LAKE__FILE")) 
    }
)
