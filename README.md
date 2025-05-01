# Big Data Final Project - Team 14

This project implements a multi-stage big data pipeline, processing job description data from Kaggle. The pipeline covers data collection, storage in PostgreSQL and HDFS, Hive data warehousing with optimizations, exploratory data analysis (EDA) using Spark SQL, predictive modeling using Spark ML, and preparation for dashboarding.

## Project Stages

The project is divided into four stages, automated by scripts in the `scripts/` directory:

1.  **Stage 1: Data Collection and Storage (`scripts/stage1.sh`)**
    *   Collects job description data from Kaggle (`scripts/data_collection.sh`).
    *   Sets up a PostgreSQL database (`team14_projectdb`) with the `job_descriptions` table (`sql/create_tables.sql`) and loads data (`scripts/build_projectdb.py`, `sql/import_data.sql`).
    *   Imports data from PostgreSQL to HDFS (`project/warehouse`) as Snappy-compressed Avro files using Sqoop.
    *   Saves Avro schema (`.avsc`) and generated Java code (`.java`) to `output/`.
    *   Puts the Avro schema into HDFS (`project/warehouse/avsc`).
    *   *Details in:* `docs/report_1.md`

2.  **Stage 2: Hive Data Warehousing & EDA (`scripts/stage2.sh`)**
    *   Creates a Hive database (`team14_projectdb`) and a partitioned (`work_type`), bucketed (`preference`) external table `job_descriptions_part` using Avro format (`sql/db.hql`, executed by `scripts/create_hive_tables.sh`). Output logged to `output/hive_results.txt`.
    *   Performs EDA using Spark SQL on the Hive table (`scripts/run_hive_queries.py` executing queries from `sql/q*.hql`).
    *   Stores EDA results in PostgreSQL tables (`q*_results`).
    *   Exports EDA results from PostgreSQL to local CSV files in `output/`.
    *   *Details in:* `docs/report_2.md` (Note: Visualization charts in `output/` are manually created).

3.  **Stage 3: Predictive Data Analytics (`scripts/stage3.sh`)**
    *   Preprocesses data using (`scripts/data_preprocessing.py`), saving train/test splits (features + log-transformed salary) to HDFS (`project/data/`).
    *   Trains, tunes (3-fold CV), and evaluates Linear Regression and GBT Regressor models using Spark ML (`scripts/ml_modeling.py`) on the log-transformed salary.
    *   Saves best models to HDFS (`project/models/`).
    *   Saves predictions (transformed back to original scale) to HDFS (`project/output/model*_predictions.csv`).
    *   Saves model evaluation comparison (log-scale RMSE/R2) to HDFS (`project/output/evaluation.csv`).
    *   Saves hyperparameter tuning results to HDFS (`project/output/*_tuning_results.csv`).
    *   Downloads models, data splits, predictions, and evaluations from HDFS to local `models/`, `data/`, and `output/` directories.
    *   *Details in:* `docs/report_3.md`

4.  **Stage 4: Presentation Preparation (`scripts/stage4.sh`)**
    *   Calculates KL Divergence between original-scale actual and predicted salaries for each model (`scripts/calculate_kl.py`), saving results to HDFS (`project/output/kl_divergence.csv`).
    *   Creates external Hive tables (`sql/stage4_hive_tables.hql`) pointing to the prediction and evaluation CSV files in HDFS for Superset access. Output logged to `output/stage4_hive_results.txt`.
    *   Downloads KL divergence results from HDFS to local `output/`.
    *   *Details in:* `docs/report_4.md` (Note: Superset dashboard creation is manual).

## Directory Structure

*   `data/`: Holds raw downloaded data and ML data splits downloaded from HDFS.
*   `docs/`: Contains project documentation (task descriptions, stage reports).
*   `models/`: Stores trained ML models downloaded from HDFS.
*   `notebooks/`: Contains Jupyter notebooks used for interactive development (if any).
*   `output/`: Stores various outputs like Avro schemas, EDA results (CSV), ML predictions (CSV), evaluations (CSV), Hive script logs.
*   `scripts/`: Contains all automation scripts (Bash, Python) for running the pipeline stages.
*   `secrets/`: Holds sensitive information like database credentials (should not be version controlled).
*   `sql/`: Contains SQL (PostgreSQL) and HQL (Hive) scripts for database/table definitions and data loading.
*   `.venv/`: Python virtual environment directory.

## Setup and Execution

1.  **Prerequisites:** Ensure necessary tools are installed (Python 3.11, Kaggle API, Hadoop client, Hive, Spark, Sqoop, PostgreSQL client). Configure access to the cluster resources (HDFS, YARN, Hive Metastore, PostgreSQL).
2.  **Secrets:** Place the PostgreSQL password in `secrets/.psql.pass`.
3.  **Environment:** Activate the Python virtual environment: `source .venv/bin/activate`. Install dependencies: `pip install -r requirements.txt`.
4.  **Run Pipeline:** Execute the stage scripts sequentially:
    ```bash
    bash scripts/stage1.sh
    bash scripts/stage2.sh
    bash scripts/stage3.sh
    bash scripts/stage4.sh
    ```
    Or run the main script:
    ```bash
    bash main.sh
    ```

Refer to the `docs/report_*.md` files for detailed implementation specifics for each stage. 