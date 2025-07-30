from pyspark.sql import SparkSession
import logging
import os

logger = logging.getLogger(__name__)

def extract_data_spark(file_name: str, **kwargs):
    data_path = os.path.join('/opt/airflow/data', file_name)

    try:
        spark = SparkSession.builder \
            .appName("DataExtraction") \
            .getOrCreate()

        df = spark.read.option("header", "true").csv(data_path)
        logger.info(f"[EXTRACT - SPARK] Successfully extracted data from {file_name}")
        return df
    except Exception as e:
        logger.error(f"[EXTRACT - SPARK] Failed to extract data from {file_name}: {e}")
        raise
