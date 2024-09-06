from dagster import Definitions

from .assets import poc_chess_lake, poc_chess_seed
from .resources import dlt_resource

# poc_ecom_assets = load_assets_from_modules([poc_ecom])

defs = Definitions(
    assets    = [
        poc_chess_seed.dlt_asset_factory,
        poc_chess_lake.dlt_asset_factory,
    ],
    resources = {
        "dlt": dlt_resource,
    }
)
