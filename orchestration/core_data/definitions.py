from dagster import Definitions, load_assets_from_modules
from dagster_embedded_elt.dlt import DagsterDltResource

from .assets import poc_chess_seed, poc_chess_lake

defs = Definitions(
    assets    = load_assets_from_modules([poc_chess_seed, poc_chess_lake]),
    resources = {
        "dlt": DagsterDltResource(),
    }
)
