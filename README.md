
# ETL Pipeline for Tech Companies Dataset

This Github marks a data engineering project that demonstrates the end-to-end ETL process using modern data engineering tools, including **Apache Airflow**, **PostgreSQL**, and **Docker**. The pipeline extracts data from CSV files, loads it into a PostgreSQL database hosted on **Neon**, and orchestrates the workflow using Airflow.

---

## Project Overview

This project simulates a real-world ETL pipeline for ingesting and managing data regarding the top tech companies from three different sectors, as well as overall tech companies, based on total revenue from 2024:

- Consumer Electronics
- Cybersecurity
- Semiconductors
- General Tech Companies

The goal here is to showcase data ingestion, transformation, and orchestration using production-grade tools, as well as to demonstrate skills in ETL Pipeline development and data engineering.

---

## Tools/Skills Used

- **Python** – scripting and ETL Pipeline development
- **Python** – ETL scripting - main version used in project
- **Airflow** – workflow orchestration and DAGs
- **PostgreSQL (Neon)** – cloud-hosted relational database
- **Docker** – containerization of services
- **SQL** – data modeling and querying
- **SQL** – ETL scripting (alternative version to demonstrate Spark skills)
---

## Project Structure:
etl_project/
├── DAGs/
│   └── etl_pipeline.py
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── data/
│   └── *.csv
├── config/
│   └── db_config.yaml
├── sql/
│   └── create_tables.sql           (for SQL proficiency demonstration, not required though)
├── docker-compose.yaml
├── Dockerfile
├── SparkScripts/       (for demonstrating Spark proficiency, not used for actual pipeline)
│   ├── extract_spark.py
│   ├── transform_spark.py
│   └── load_spark.py
