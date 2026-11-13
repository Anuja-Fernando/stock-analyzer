import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from .settings import DBT_PROJECT_DIR

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def portfolio_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()   # seed + run + test


dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=str(DBT_PROJECT_DIR))