from pyspark.sql import SparkSession
import os
import yaml
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def load_data_spark(file_name: str, **kwargs):
    try:
        # Load DB config
        config_path = os.path.join('/opt/airflow/config', 'db_config.yaml')
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        db_url = config['neon']['connection_string']
        table_name = os.path.splitext(file_name)[0]

        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("DataLoading") \
            .getOrCreate()

        # Read CSV into Spark DataFrame
        data_path = os.path.join('/opt/airflow/data', file_name)
        df = spark.read.option("header", "true").csv(data_path)

        # Write to PostgreSQL using JDBC
        df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("user", config['neon']['user']) \
            .option("password", config['neon']['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        logger.info(f"[LOAD - SPARK] Successfully loaded data into table: {table_name}")

    except Exception as e:
        logger.error(f"[LOAD - SPARK] Failed to load data from {file_name} into Neon DB: {e}")
        raise
