"""
utils/control_table_manager.py

Handles all reads and updates to the config.file_sources control table.
The rest of the framework interacts with the control table only through this class.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import logging

logger = logging.getLogger(__name__)


class ControlTableManager:

    def __init__(self, spark: SparkSession, env: str = "prod"):
        self.spark = spark
        self.env = env
        self.control_table = f"{env}_catalog.config.file_sources"

    def get_active_sources(self) -> list:
        """Return all active sources as a list of dicts."""
        df = self.spark.table(self.control_table).filter("is_active = true")
        return [row.asDict() for row in df.collect()]

    def get_source(self, source_id: str) -> dict:
        """Return a single source config by source_id."""
        df = (self.spark.table(self.control_table)
              .filter(f"source_id = '{source_id}'"))
        rows = df.collect()
        if not rows:
            raise ValueError(f"Source '{source_id}' not found in control table.")
        return rows[0].asDict()

    def update_watermark(self, source_id: str):
        """Update last_loaded_at to current timestamp after a successful run."""
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET last_loaded_at = current_timestamp(),
                updated_at = current_timestamp()
            WHERE source_id = '{source_id}'
        """)
        logger.info(f"Watermark updated for source: {source_id}")

    def deactivate_source(self, source_id: str):
        """Soft-delete a source by setting is_active = false."""
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET is_active = false, updated_at = current_timestamp()
            WHERE source_id = '{source_id}'
        """)
        logger.info(f"Source deactivated: {source_id}")

    def register_source(self, source_config: dict):
        """
        Register a new source by inserting a row into the control table.
        source_config must include: source_id, source_path, file_format,
        target_table, load_type.
        """
        required = ["source_id", "source_path", "file_format", "target_table", "load_type"]
        for field in required:
            if field not in source_config:
                raise ValueError(f"Missing required field: {field}")

        row_df = self.spark.createDataFrame([source_config])
        row_df.write.format("delta").mode("append").saveAsTable(self.control_table)
        logger.info(f"Registered new source: {source_config['source_id']}")
