from dagster import Definitions, load_assets_from_modules
from dagster_embedded_elt.dlt import DagsterDltResource

from . import assets

# TODO: re-enable auto-asset discovery
# all_assets = load_assets_from_modules([assets])
# all_assets.extend(assets.dlt_assets)

dlt_resources = DagsterDltResource()

defs = Definitions(
    assets    = [
        assets.dlt_assets
    ],
    resources = {
        "dlt": dlt_resources,
    }
)
