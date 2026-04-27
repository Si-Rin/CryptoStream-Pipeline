import sys
import os
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # 3 levels up from orchestration/flow/
sys.path.insert(0, str(PROJECT_ROOT))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from prefect import flow, task
from prefect.logging import get_run_logger
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
from ingestion.producer_rest.coingecko_global_metrics import get_global_metrics

import os
DELTA_PATH = os.path.abspath("data/bronze/global_metrics")

@task(retries=3, retry_delay_seconds=60)
def fetch_snapshot():
    logger = get_run_logger()
    df = get_global_metrics()
    if df.empty:
        raise ValueError("Empty response from CoinGecko")
    logger.info(f"Fetched {len(df)} rows")
    return df

@task
def merge_to_delta(pdf):
    logger = get_run_logger()
    builder = SparkSession.builder.appName("batch_global_metrics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    sdf = spark.createDataFrame(pdf)
    
    if DeltaTable.isDeltaTable(spark, DELTA_PATH):
        DeltaTable.forPath(spark, DELTA_PATH).alias("t").merge(
            sdf.alias("s"),
            "t.snapshot_hour = s.snapshot_hour"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("MERGE completed")
    else:
        sdf.write.format("delta").save(DELTA_PATH)
        logger.info("Initial table created")
    
    spark.stop()

@flow(name="batch-global-metrics")
def batch_global_metrics_flow():
    pdf = fetch_snapshot()
    merge_to_delta(pdf)

if __name__ == "__main__":
    batch_global_metrics_flow()