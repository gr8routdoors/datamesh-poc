from dagster import Definitions, load_assets_from_modules

from .assets import poc_ecom
from .resources import dlt_resource

# poc_ecom_assets = load_assets_from_modules([poc_ecom])

defs = Definitions(
    assets    = [
        poc_ecom.dlt_asset_factory
    ],
    resources = {
        "dlt": dlt_resource,
    }
)
