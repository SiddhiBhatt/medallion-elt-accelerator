"""
ingestion/bronze/file_watcher_ingestor.py

Control-table-driven ingestion engine for the Bronze layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from utils.control_table_manager import ControlTableManager
from utils.audit_logger import AuditLogger
import logging

logger = logging.getLogger(__name__)

class FileWatcherIngestor:

    def __init__(self, spark: SparkSession, env: str = "prod"):
        self.spark = spark
        self.env = env
        self.ctrl = ControlTableManager(spark, env)

    def run_all(self):
        sources = self.ctrl.get_active_sources()
        logger.info(f"Found {len(sources)} active sources.")
        for source in sources:
            self._process_source(source)

    def run_source(self, source_id: str):
        source = self.ctrl.get_source(source_id)
        self._process_source(source)

    def _process_source(self, source: dict):
        try:
            df = self._read_source(source)
            df = self._add_metadata(df, source)
            if source["load_type"] == "incremental":
                df = self._apply_watermark(df, source)
            self._write_bronze(df, source)
            self.ctrl.update_watermark(source["source_id"])
            logger.info(f"[{source['source_id']}] Done. Rows: {df.count()}")
        except Exception as e:
            logger.error(f"[{source['source_id']}] Failed: {str(e)}")
            raise

    def _read_source(self, source: dict):
        fmt = source["file_format"]
        path = source["source_path"]
        opts = {}
        if fmt == "csv":
            opts["header"] = str(source.get("header", True))
            if source.get("delimiter"):
                opts["delimiter"] = source["delimiter"]
            opts["inferSchema"] = "true"
        return self.spark.read.format(fmt).options(**opts).load(path)

    def _add_metadata(self, df, source: dict):
        return (df
            .withColumn("_source_file", input_file_name())
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_id", lit(source["source_id"]))
            .withColumn("_load_type", lit(source["load_type"]))
        )

    def _apply_watermark(self, df, source: dict):
        wm_col = source.get("watermark_column")
        last_loaded = source.get("last_loaded_at")
        if wm_col and last_loaded:
            return df.filter(f"{wm_col} > '{last_loaded}'")
        return df

    def _write_bronze(self, df, source: dict):
        (df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(source["target_table"])
        )
