# Imports all DBT assets

from collections.abc import Mapping
from typing import Any, Optional

from dagster import AssetExecutionContext, AssetKey, SourceAsset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from ..projects import dbt_project


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def clean_schema_name(self, schema_name: str) -> str:
        # schema gets prefixed with `main_` in duckdb so we use the config instead
        return schema_name.removeprefix('main_')
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return self.clean_schema_name(dbt_resource_props["schema"])
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_database = dbt_resource_props["database"]
        resource_schema = self.clean_schema_name(dbt_resource_props["schema"])
        resource_name = dbt_resource_props["name"]
        resource_type = dbt_resource_props["resource_type"]

        # if metadata has been provided in the yaml use that, otherwise construct key
        if (
            resource_type == "source"
            and "meta" in dbt_resource_props
            and "dagster" in dbt_resource_props["meta"]
            and "asset_key" in dbt_resource_props["meta"]["dagster"]
        ):
            return AssetKey(dbt_resource_props["meta"]["dagster"]["asset_key"])

        return AssetKey(f"{resource_database}__{resource_schema}__{resource_name}")

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def dbt_non_partitioned_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from (
        dbt.cli(["build"], context=context)
        .stream()
        .fetch_row_counts()
        .fetch_column_metadata()
    )
