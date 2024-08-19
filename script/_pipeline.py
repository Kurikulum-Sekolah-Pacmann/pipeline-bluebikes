from _staging_pipeline import pipeline_stg
from _warehouse_pipeline import pipeline_wh


if __name__ == "__main__":
    pipeline_stg()
    pipeline_wh()