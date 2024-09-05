from collections.abc import Iterable
from dagster import AssetKey
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
)

class PrefixDagsterDltTranslator(DagsterDltTranslator):
    """Allows for setting custom prefixes for DLT assets"""

    def __init__(self, asset_prefix, upstream_prefix):
        self.asset_prefix = asset_prefix
        self.upstream_prefix = upstream_prefix    
    
    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        return AssetKey(f"{self.asset_prefix}__{resource.name}")

    def get_deps_asset_keys(self, resource: DagsterDltResource) -> Iterable[AssetKey]:
        """Overrides upstream asset key to be a single source asset."""
        return [AssetKey(f"{self.upstream_prefix}__{resource.name}")]
