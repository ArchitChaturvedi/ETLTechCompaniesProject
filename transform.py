import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def transform_data(file_name: str, **kwargs):

    data_path = os.path.join('/opt/airflow/data', file_name)

    try:
        df = pd.read_csv(data_path, encoding='utf-8')

        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        df.drop_duplicates(inplace=True)

        df.fillna("Unknown", inplace=True)

        df['source_file'] = file_name
        df['ingestion_timestamp'] = pd.Timestamp.now()

        logger.info(f"[TRANSFORM] Successfully transformed data from {file_name}")
        return df

    except Exception as e:
        logger.error(f"[TRANSFORM] Failed to transform data from {file_name}: {e}")
        raise
