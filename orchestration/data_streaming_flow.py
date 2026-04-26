"""
data_streaming_flow.py — Prefect orchestration for the crypto medallion pipeline.

Architecture:
    Bronze (Kafka → Delta)
        ↓
    Silver (Bronze Delta → OHLCV Delta)
        ↓
    Gold   (Silver Delta → Indicators Delta)

Each layer runs as a Prefect task wrapping the existing job scripts.
Sequential execution is enforced by Prefect's dependency graph —
Silver cannot start until Bronze succeeds, Gold cannot start until
Silver succeeds.
We used this architecture to meet the requirement of task dependency management

Usage:
    # 1. Start the Prefect UI server (keep this running in a terminal)
    prefect server start

    # 2. In a second terminal, point the client at the local server
    prefect config set PREFECT_API_URL=http://localhost:4200/api

    # 3. Run once manually to verify everything works
    python orchestration/data_streaming_flow.py

    # 4. Deploy with a 60-second schedule
    prefect deployment build orchestration/data_streaming_flow.py:crypto_pipeline \
        --name "crypto-dev" \
        --interval 60 \
        --apply

    # 5. Start the agent that will execute scheduled runs
    prefect agent start -q default

    # 6. Open the UI
    #    http://localhost:4200
"""

import subprocess
import sys
import os
from pathlib import Path
from datetime import timedelta

from prefect import flow, task
import logging
logger = logging.getLogger(__name__)

# ─── PATH RESOLUTION ─────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).parent.parent.resolve()  # two levels up from orchestration/ to project root
JOBS_DIR = ROOT_DIR /  "streaming" / "spark"  # relative path to the folder containing the job scripts
PYTHON   = sys.executable              # path to the current Python interpreter, ensures we use the same environment for subprocesses


# ─── HELPER ──────────────────────────────────────────────────────────────────
def _run_script(script_name: str, logger) -> None:
    """
    Runs a Python script as a subprocess, streams its stdout/stderr
    line-by-line into the Prefect task log, and raises on non-zero exit.

    Using subprocess instead of importing the module directly keeps each
    Spark job isolated — they each call get_spark_session() and manage
    their own SparkContext lifecycle without interfering with each other
    or with the Prefect process.
    """
    script_path = JOBS_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    logger.info(f"Launching: {script_path}")

    process = subprocess.Popen(
        [PYTHON, str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,          # merge stderr into stdout
        text=True,
        cwd=str(ROOT_DIR),                 # run from project root so .env is found
    )

    # Stream output line by line so it appears in the Prefect UI in real time
    for line in process.stdout:
        logger.info(line.rstrip())

    process.wait()

    if process.returncode != 0:
        raise RuntimeError(
            f"{script_name} exited with code {process.returncode}"
        )

    logger.info(f"{script_name} completed successfully.")


# ─── TASKS ───────────────────────────────────────────────────────────────────

@task(
    name="Bronze — Kafka to Delta",
    description=(
        "Reads new Kafka offsets (all 5 crypto topics), parses trade JSON, "
        "appends raw rows to the Bronze Delta table. "
        "Uses Trigger.availableNow=True — exits when all available offsets "
        "are committed."
    ),
    retries=2,
    retry_delay_seconds=15,
    timeout_seconds=300,               # fail the task if Bronze hangs > 5 min
)
def task_bronze() -> None:
    logger.info("=== BRONZE START ===")
    _run_script("bronze_streaming.py", logger)
    logger.info("=== BRONZE DONE ===")


@task(
    name="Silver — Deduplicate & OHLCV",
    description=(
        "Reads Bronze Delta, deduplicates by trade_id, aggregates into "
        "1-minute OHLCV candles per symbol, appends to Silver Delta. "
        "Uses Trigger.availableNow=True."
    ),
    retries=2,
    retry_delay_seconds=15,
    timeout_seconds=300,
)
def task_silver() -> None:
    logger.info("=== SILVER START ===")
    _run_script("silver_streaming.py", logger)
    logger.info("=== SILVER DONE ===")


@task(
    name="Gold — Technical Indicators",
    description=(
        "Reads all Silver OHLCV rows in batch, computes RSI(14), "
        "MACD-approx(12/26/9), Bollinger Bands(20) per symbol, "
        "overwrites Gold Delta table."
    ),
    retries=1,                         # Gold is idempotent (overwrite), safe to retry
    retry_delay_seconds=10,
    timeout_seconds=180,
)
def task_gold() -> None:
    logger.info("=== GOLD START ===")
    _run_script("gold_streaming.py", logger)
    logger.info("=== GOLD DONE ===")


# ─── FLOW ────────────────────────────────────────────────────────────────────

@flow(
    name="crypto-pipeline",
    description=(
        "Medallion pipeline: Bronze (Kafka) → Silver (OHLCV) → Gold (Indicators). "
        "Scheduled every 60 seconds. Sequential execution enforced by task ordering."
    ),
    # If a cycle takes longer than 90s, Prefect marks it failed rather than
    # letting it pile up behind the next scheduled run.
    timeout_seconds=90,
)
def crypto_pipeline() -> None:
    """
    Main flow. Calling tasks sequentially here is what creates the
    dependency graph visible in the Prefect UI.

    Prefect guarantees:
      - task_silver() will not start if task_bronze() failed or was skipped.
      - task_gold()   will not start if task_silver() failed or was skipped.

    This is the "gestion des dépendances entre tâches" requirement.
    """
    logger.info("Pipeline cycle starting.")

    task_bronze()   # blocks until Bronze script exits
    task_silver()   # only reached if Bronze succeeded
    task_gold()     # only reached if Silver succeeded

    logger.info("Pipeline cycle complete.")


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    crypto_pipeline.serve(
        name="crypto-dev",
        interval=1800,         # run every 30 minutes
        pause_on_shutdown=False,
    )