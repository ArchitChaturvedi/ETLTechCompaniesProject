import pandas as pd
import os

def extract_data(file_name: str, **kwargs):
    """
    Extracts data from a CSV file located in the /data directory.

    Args:
        file_name (str): Name of the CSV file to extract (e.g., 'tech_companies_2024.csv').

    Returns:
        pd.DataFrame: Extracted raw data as a DataFrame.
    """
    data_path = os.path.join('/opt/airflow/data', file_name)

    try:
        df = pd.read_csv(data_path)
        print(f"[EXTRACT] Successfully extracted data from {file_name}")
        return df
    except Exception as e:
        print(f"[EXTRACT] Failed to extract data from {file_name}: {e}")
        raise
