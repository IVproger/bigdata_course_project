# SQL and HQL Scripts (`sql/`)

This directory contains SQL (PostgreSQL) and HQL (HiveQL) scripts used for defining database schemas, tables, and performing data manipulation or querying tasks across different stages.

## Contents

### Stage 1 (PostgreSQL Setup)

*   `create_tables.sql`: Defines the schema for the `job_descriptions` table in the PostgreSQL database (`team14_projectdb`). Includes constraints for data validation. Used by `scripts/build_projectdb.py`.
*   `import_data.sql`: Contains the PostgreSQL `COPY` command used by `scripts/build_projectdb.py` to load data efficiently from the local `data/job_descriptions.csv` file into the `job_descriptions` table.
*   `test_database.sql`: Contains SQL queries used within `scripts/build_projectdb.py` to verify the database creation and data loading in PostgreSQL.

### Stage 2 (Hive Setup & EDA)

*   `db.hql`: HiveQL script executed by `scripts/create_hive_tables.sh` (via `beeline`) to:
    *   Create the Hive database `team14_projectdb`.
    *   Create an initial external table pointing to the Stage 1 Sqoop output (temporarily).
    *   Create the final partitioned (`work_type`) and bucketed (`preference`) external table `job_descriptions_part` stored as Avro with Snappy compression.
    *   Insert data into `job_descriptions_part` from the initial table.
    *   Drop the initial temporary table.
    *   Includes settings like `hive.exec.dynamic.partition=true`.
*   `q1.hql` - `q6.hql`: HiveQL scripts containing the Exploratory Data Analysis (EDA) queries. These are read and executed using Spark SQL by the `scripts/run_hive_queries.py` script.

### Stage 4 (Hive for Presentation)

*   `stage4_hive_tables.hql`: HiveQL script executed by `scripts/stage4.sh` (via `beeline`) to create external Hive tables (`evaluation_results`, `model1_predictions`, `model2_predictions`, `kl_divergence`) pointing to the CSV result files stored in HDFS (`project/output/`). This makes the ML results queryable for tools like Apache Superset.

Refer to the `docs/report_*.md` files for context on how and when each script is executed. 