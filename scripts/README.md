# Automation Scripts (`scripts/`)

This directory contains all the shell and Python scripts used to automate the different stages of the big data pipeline.

## Main Stage Scripts

These scripts orchestrate the execution of each pipeline stage:

*   `stage1.sh`: Automates Stage 1 - Data collection, PostgreSQL setup, and Sqoop import to HDFS.
*   `stage2.sh`: Automates Stage 2 - Hive table creation, Spark SQL EDA, PostgreSQL result storage, and CSV export.
*   `stage3.sh`: Automates Stage 3 - Spark ML preprocessing, model training/tuning/evaluation, and saving/downloading results/models from HDFS.
*   `stage4.sh`: Automates Stage 4 - KL Divergence calculation (Spark), Hive external table creation for results, and downloading KL results from HDFS.

## Supporting Scripts

These scripts are called by the main stage scripts or perform specific tasks within a stage:

*   `data_collection.sh`: (Stage 1) Downloads the dataset from Kaggle using the Kaggle API.
*   `build_projectdb.py`: (Stage 1) Connects to PostgreSQL, creates tables (using `sql/create_tables.sql`), and imports data from the local CSV (using `sql/import_data.sql`).
*   `create_hive_tables.sh`: (Stage 2) Executes the Hive setup script (`sql/db.hql`) using `beeline`.
*   `run_hive_queries.py`: (Stage 2) Uses Spark SQL to run EDA HiveQL queries (`sql/q*.hql`), writes results to PostgreSQL, and exports them to local CSV files (`output/q*.csv`).
*   `data_preprocessing.py`: (Stage 3) PySpark script for preprocessing data (feature engineering, scaling, encoding, train/test split) and saving results to HDFS (`project/data/`).
*   `ml_modeling.py`: (Stage 3) PySpark script for loading preprocessed data, training/tuning ML models (Linear Regression, GBT), evaluating them, and saving models/predictions/evaluations to HDFS.
*   `calculate_kl.py`: (Stage 4) PySpark script to calculate KL divergence between actual and predicted salaries (original scale) using prediction files from HDFS, saving results back to HDFS.

Refer to the `docs/report_*.md` files for details on how these scripts are used in each stage. 