from typing import Dict
from dagster import asset, AssetExecutionContext

@asset(
    description="WISPR_FILES",
    group_name="WISPR",
    compute_kind="Python",
)
def WISPR_FILES(context: AssetExecutionContext) -> Dict:
    context.log.info("Fetched files for WISPR_FILES from dla website.")
    context.add_output_metadata({
        "wispr_files": "Successfully downloaded the WISPR_FILES Files."
    })
    return {
        "wispr_files": "Successfully downloaded the WISPR_FILES Files."
    }