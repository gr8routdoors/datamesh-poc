from collections.abc import Iterable
from dagster import AssetKey
from dagster_embedded_elt.dlt import (
    DagsterDltResource,
    DagsterDltTranslator,
)

class CanonicalDagsterDltTranslator(DagsterDltTranslator):
    """Allows for setting custom prefixes for DLT assets"""

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
        return [AssetKey(f"{self.source_name}__{self.database_name}__{self.schema_name}__{resource.name}")]
