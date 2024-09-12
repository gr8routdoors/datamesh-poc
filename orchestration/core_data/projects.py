from pathlib import Path

from dagster_dbt import DbtProject


# DBT Project integration
RELATIVE_DBT_PROJECT_PATH = "transforms"
dbt_project_dir = Path(__file__).parent.joinpath(RELATIVE_DBT_PROJECT_PATH).resolve()
dbt_project     = DbtProject(project_dir=dbt_project_dir)
# If `dagster dev` is used, the dbt project will be prepared to create the manifest at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
dbt_project.prepare_if_dev()
