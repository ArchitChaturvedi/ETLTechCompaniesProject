import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def transform_data(file_name: str, **kwargs):
    """
    Transforms the extracted data by cleaning and standardizing it.

    Args:
        file_name (str): Name of the CSV file being processed.

    Returns:
        pd.DataFrame: Transformed DataFrame ready for loading.
    """
    data_path = os.path.join('/opt/airflow/data', file_name)

    try:
        # Load the raw data
        df = pd.read_csv(data_path, encoding='utf-8')

        # Example transformations:
        # 1. Standardize column names
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # 2. Drop duplicates
        df.drop_duplicates(inplace=True)

        # 3. Handle missing values (example: fill with 'Unknown' or drop)
        df.fillna("Unknown", inplace=True)

        # 4. Add metadata
        df['source_file'] = file_name
        df['ingestion_timestamp'] = pd.Timestamp.now()

        logger.info(f"[TRANSFORM] Successfully transformed data from {file_name}")
        return df

    except Exception as e:
        logger.error(f"[TRANSFORM] Failed to transform data from {file_name}: {e}")
        raise
