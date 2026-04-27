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
from ingestion.producer_rest.coingecko_market_snapshot import get_market_snapshot

DELTA_PATH = os.path.abspath("data/bronze/market_snapshot")

@task(retries=3, retry_delay_seconds=60)
def fetch_snapshot():
    logger = get_run_logger()
    df = get_market_snapshot()
    if df.empty:
        raise ValueError("Empty response from CoinGecko")
    logger.info(f"Fetched {len(df)} rows")
    return df

@task
def merge_to_delta(pdf):
    logger = get_run_logger()
    
    # Write pandas to temp parquet first (avoids Python worker socket)
    import tempfile
    tmp_parquet = os.path.join(tempfile.gettempdir(), "snapshot_tmp.parquet")
    pdf.to_parquet(tmp_parquet, index=False)
    
    builder = SparkSession.builder.appName("batch_market_snapshot") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    sdf = spark.read.parquet(tmp_parquet)
    
    if DeltaTable.isDeltaTable(spark, DELTA_PATH):
        DeltaTable.forPath(spark, DELTA_PATH).alias("t").merge(
            sdf.alias("s"),
            "t.symbol = s.symbol AND t.snapshot_hour = s.snapshot_hour"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("MERGE completed")
    else:
        sdf.write.format("delta").save(DELTA_PATH)
        logger.info("Initial table created")
    
    spark.stop()
    os.remove(tmp_parquet)
    
@flow(name="batch-market-snapshot")
def batch_market_snapshot_flow():
    pdf = fetch_snapshot()
    merge_to_delta(pdf)

if __name__ == "__main__":
    batch_market_snapshot_flow()