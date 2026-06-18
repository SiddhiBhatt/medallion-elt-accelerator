"""
ingestion/bronze/file_watcher_ingestor.py

Control-table-driven ingestion engine for the Bronze layer.
Reads all active source configs from config.file_sources and
processes each one without any hardcoded paths or schemas.

Design principles:
  - Zero code changes to onboard a new source (add a control table row)
  - Full audit trail per run
  - Incremental + full load modes
  - Schema evolution handled via Delta mergeSchema
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from utils.control_table_manager import ControlTableManager
from utils.audit_logger import AuditLogger
from utils.delta_utils import merge_into_delta
import logging

logger = logging.getLogger(__name__)


class FileWatcherIngestor:
    """
    Core ingestion engine. Reads source configs from control table,
    ingests each active source into its Bronze Delta table.
    """

    def __init__(self, spark: SparkSession, env: str = "prod"):
        self.spark = spark
        self.env = env
        self.ctrl = ControlTableManager(spark, env)
        self.audit = AuditLogger(spark, env)

    def run_all(self):
        """Process all active sources from the control table."""
        sources = self.ctrl.get_active_sources()
        logger.info(f"Found {len(sources)} active sources to process.")
        results = []
        for source in sources:
            result = self._process_source(source)
            results.append(result)
        return results

    def run_source(self, source_id: str):
        """Process a single source by ID — useful for reruns and targeted loads."""
        source = self.ctrl.get_source(source_id)
        return self._process_source(source)

    def _process_source(self, source: dict) -> dict:
        source_id = source["source_id"]
        run_id = self.audit.start_run(source_id, layer="bronze")
        logger.info(f"[{source_id}] Starting ingestion run {run_id}")

        try:
            df = self._read_source(source)
            df = self._add_metadata(df, source)

            if source["load_type"] == "incremental":
                df = self._apply_watermark(df, source)

            row_count = df.count()
            self._write_bronze(df, source)

            self.ctrl.update_watermark(source_id)
            self.audit.complete_run(run_id, rows_processed=row_count)
            logger.info(f"[{source_id}] Completed. Rows written: {row_count}")
            return {"source_id": source_id, "status": "success", "rows": row_count}

        except Exception as e:
            self.audit.fail_run(run_id, error_message=str(e))
            logger.error(f"[{source_id}] Failed: {str(e)}")
            raise

    def _read_source(self, source: dict):
        fmt = source["file_format"]
        path = source["source_path"]
        read_opts = {}

        if fmt == "csv":
            read_opts["header"] = str(source.get("header", True))
            if source.get("delimiter"):
                read_opts["delimiter"] = source["delimiter"]
            read_opts["inferSchema"] = "true"

        return self.spark.read.format(fmt).options(**read_opts).load(path)

    def _add_metadata(self, df, source: dict):
        """Add framework metadata columns to every ingested record."""
        return (df
            .withColumn("_source_file", input_file_name())
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_id", lit(source["source_id"]))
            .withColumn("_load_type", lit(source["load_type"]))
        )

    def _apply_watermark(self, df, source: dict):
        """Filter to only new records based on watermark column."""
        wm_col = source.get("watermark_column")
        last_loaded = source.get("last_loaded_at")

        if wm_col and last_loaded:
            logger.info(f"Applying watermark filter: {wm_col} > {last_loaded}")
            return df.filter(f"{wm_col} > '{last_loaded}'")
        return df

    def _write_bronze(self, df, source: dict):
        """Append records to the Bronze Delta table with schema evolution enabled."""
        target = source["target_table"]
        (df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target)
        )
        logger.info(f"Written to {target}")
