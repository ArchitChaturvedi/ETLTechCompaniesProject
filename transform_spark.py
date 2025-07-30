from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import logging
import os

logger = logging.getLogger(__name__)

def transform_data_spark(file_name: str, **kwargs):
    data_path = os.path.join('/opt/airflow/data', file_name)

    try:
        spark = SparkSession.builder \
            .appName("DataTransformation") \
            .getOrCreate()

        df = spark.read.option("header", "true").csv(data_path)

        for oldname in df.columns:
            newname = oldname.strip().lower().replace(" ", "_")
            df = df.withColumnRenamed(oldname, newname)

        df = df.dropDuplicates()

        df = df.fillna("Unknown")

        df = df.withColumn("source_file", lit(file_name))
        df = df.withColumn("ingestion_timestamp", current_timestamp())

        logger.info(f"[TRANSFORM - SPARK] Successfully transformed data from {file_name}")
        return df

    except Exception as e:
        logger.error(f"[TRANSFORM - SPARK] Failed to transform data from {file_name}: {e}")
        raise
