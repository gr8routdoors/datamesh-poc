# Imports all DBT assets

from collections.abc import Mapping
from typing import Any, Optional

from dagster import AssetExecutionContext, AssetKey, SourceAsset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from ..projects import dbt_project


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        # schema gets prefixed with `main_` in duckdb so we use the config instead
        return dbt_resource_props["schema"]
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return AssetKey(f"{dbt_resource_props["database"]}__{dbt_resource_props["schema"]}__{dbt_resource_props["name"]}")

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def transforms_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Correct the group name from `default` for upstream dependencies
assets_chess_sources     = [
    SourceAsset(key, group_name="bronze") for key in transforms_assets.dependency_keys
]
