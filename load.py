import pandas as pd
import os
from sqlalchemy import create_engine
import yaml
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

def load_data(file_name: str, **kwargs):
    
    try:
        config_path = os.path.join('/opt/airflow/config', 'db_config.yaml')
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        db_url = config['neon']['connection_string']
        engine = create_engine(db_url)

        data_path = os.path.join('/opt/airflow/data', file_name)
        df = pd.read_csv(data_path)

        table_name = os.path.splitext(file_name)[0]

        df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi')

        logger.info(f"[LOAD] Successfully loaded data into table: {table_name}")

    except Exception as e:
        logger.error(f"[LOAD] Failed to load data from {file_name} into Neon DB: {e}")
        raise
