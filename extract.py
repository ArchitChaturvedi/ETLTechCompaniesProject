import pandas as pd
import logging 
import os

logger = logging.getLogger(__name__)

def extract_csv(file_name: str, **kwargs):
    
    data_path = os.path.join('/opt/airflow/data', file_name)

    try:
        df = pd.read_csv(data_path)
        logger.info(f"[EXTRACT] Successfully extracted data from {file_name}")
        return df
    except Exception as e:
        logger.error(f"[EXTRACT] Failed to extract data from {file_name}: {e}")
        raise
