from dagster import Definitions
from pipeline.asset.asset_defs import WISPR_FILES

defs = Definitions(
    assets=[
        WISPR_FILES
    ]
)