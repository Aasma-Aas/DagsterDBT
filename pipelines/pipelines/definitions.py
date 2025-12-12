from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import analytics_dbt_assets
from .project import analytics_project
from .schedules import schedules

defs = Definitions(
    assets=[analytics_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=analytics_project),
    },
)

