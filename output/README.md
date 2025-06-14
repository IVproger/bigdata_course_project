# Output Files (`output/`)

This directory stores various output files generated during the execution of the different pipeline stages.

## Contents by Stage

### Stage 1 (Data Collection & Storage)

*   `*.avsc`: Avro schema files generated by the `sqoop import-all-tables` command for tables imported from PostgreSQL (e.g., `job_descriptions.avsc`).
*   `*.java`: Java source code files generated by Sqoop for interacting with the Avro data (e.g., `job_descriptions.java`).

### Stage 2 (Hive & EDA)

*   `q*.csv`: CSV files containing the results of the Exploratory Data Analysis queries (q1-q6), exported from PostgreSQL.
*   `hive_results.txt`: Log output from the `beeline` execution of `sql/db.hql` (Hive database and table creation).
*   `q*.jpg` / `q*.png`: Manually generated chart images from Apache Superset visualizing the EDA results stored in PostgreSQL (as mentioned in `docs/report_2.md`).

### Stage 3 (Predictive Data Analytics)

*   `model1_predictions.csv`: Predictions from the best Linear Regression model on the test set (original salary scale), downloaded from HDFS.
*   `model2_predictions.csv`: Predictions from the best GBT Regressor model on the test set (original salary scale), downloaded from HDFS.
*   `evaluation.csv`: Comparison of the best models (Linear Regression, GBT) based on RMSE and R2 metrics evaluated on the test data (log-transformed scale), downloaded from HDFS.
*   `lr_tuning_results.csv`: Hyperparameter tuning results (average RMSE per parameter set on log scale) for Linear Regression, downloaded from HDFS.
*   `gbt_tuning_results.csv`: Hyperparameter tuning results (average RMSE per parameter set on log scale) for GBT Regressor, downloaded from HDFS.

### Stage 4 (Presentation Preparation)

*   `kl_divergence.csv`: Calculated Kullback-Leibler (KL) divergence values comparing actual vs. predicted salary distributions (original scale) for both models, downloaded from HDFS.
*   `stage4_hive_results.txt`: Log output from the `beeline` execution of `sql/stage4_hive_tables.hql` (creation of external Hive tables for Superset).

**Note:** Most CSV files in this directory are downloaded from HDFS locations (like `project/output/`) by the stage automation scripts (`scripts/stage*.sh`), as detailed in the `docs/report_*.md` files. 