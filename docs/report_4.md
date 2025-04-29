# Stage IV - Presentation & Delivery Report

## Overview
This report details the implementation of Stage IV, focusing on preparing the results from previous stages for presentation in a dashboard and calculating additional metrics for model comparison. The primary goal is to make the analytical and modeling outputs accessible for visualization in Apache Superset.

## Implementation Steps

### 1. Data Preparation for Dashboarding
To enable Apache Superset to visualize the results generated in Stage III (ML modeling), external Hive tables were created pointing to the HDFS locations where these results were stored as CSV files.

- **Hive Tables Created (`sql/stage4_hive_tables.hql`):**
    - `evaluation_results`: Stores the RMSE and R2 metrics for the best Linear Regression and GBT models. Points to `project/output/evaluation.csv`.
    - `model1_predictions`: Stores the actual (`label`) and predicted (`prediction`) salaries from the best Linear Regression model on the test set. Points to `project/output/model1_predictions.csv`.
    - `model2_predictions`: Stores the actual (`label`) and predicted (`prediction`) salaries from the best GBT Regressor model on the test set. Points to `project/output/model2_predictions.csv`.
    - `kl_divergence`: Stores the Kullback-Leibler divergence metric calculated in the next step. Points to `project/output/kl_divergence.csv`.
- **Table Definition:** The `CREATE EXTERNAL TABLE` statements specified the schema (column names and types), the data format (TEXTFILE), the field delimiter (','), the HDFS location, and instructed Hive to skip the header row present in the CSV files.
- **Execution:** The `sql/stage4_hive_tables.hql` script is executed using `beeline` within the `scripts/stage4.sh` automation script. The output is logged to `output/stage4_hive_results.txt`.

### 2. KL Divergence Calculation (`scripts/calculate_kl.py`)
Kullback-Leibler (KL) divergence was calculated to measure the difference between the probability distribution of the predicted salaries and the probability distribution of the actual salaries in the test set.

- **Process:**
    1.  A PySpark script (`scripts/calculate_kl.py`) was developed.
    2.  It reads the prediction CSV files (`model1_predictions.csv`, `model2_predictions.csv`) from HDFS.
    3.  Determines a common range (min/max) encompassing both actual labels and predictions across both models.
    4.  Discretizes the continuous salary values (labels and predictions) into 50 bins.
    5.  Calculates the probability distribution for the actual labels (P) and the predicted labels for each model (Q1, Q2).
A small epsilon (1e-10) is added to bin probabilities to prevent issues with zero values.
    6.  Computes KL divergence \(D_{KL}(P || Q)\) for both Model 1 vs Actual and Model 2 vs Actual.
    7.  Saves the resulting KL divergence values for each model type into a new CSV file (`kl_divergence.csv`) in HDFS (`project/output/kl_divergence.csv`).
- **Execution:** The script is executed using `spark-submit` within `scripts/stage4.sh`.

### 3. Automation (`scripts/stage4.sh`)
The entire process for this stage (excluding the manual Superset dashboard creation) is automated:
- Sets up the necessary environment variables (`YARN_CONF_DIR`, `PYSPARK_PYTHON`).
- Cleans the HDFS output location for KL divergence results.
- Executes the `calculate_kl.py` Spark script.
- Executes the `stage4_hive_tables.hql` Hive script using `beeline`.
- Downloads the KL divergence results from HDFS to the local `output/` directory.
- Includes a `pylint` check for the `calculate_kl.py` script.

## Dashboard Integration (Manual Steps)
The Hive tables created (`evaluation_results`, `model1_predictions`, `model2_predictions`, `kl_divergence`) are now ready to be used as data sources within Apache Superset.

The following manual steps are required in Superset:
1.  **Connect to Hive:** Ensure Superset has a database connection configured to the Hive metastore (`jdbc:hive2://hadoop-03.uni.innopolis.ru:10000/team14_projectdb`).
2.  **Add Datasets:** Create new datasets in Superset based on the newly created Hive tables (`evaluation_results`, `model1_predictions`, `model2_predictions`, `kl_divergence`).
3.  **Create Charts:** Build charts based on these datasets and the EDA datasets from Stage II (e.g., `q1_results` to `q6_results` in PostgreSQL).
    - *Data Description:* Show table schemas (querying `information_schema` in PostgreSQL), record counts, sample data.
    - *EDA Insights:* Recreate/add charts from Stage II (`q1.jpg` - `q6.jpg`) using the PostgreSQL tables (`q1_results` - `q6_results`). Add text descriptions/conclusions.
    - *ML Modeling Results:* Create charts to visualize:
        - Evaluation metrics (RMSE, R2) from `evaluation_results`.
        - KL Divergence from `kl_divergence`.
        - Distributions of actual vs. predicted salaries (histograms) from `model1_predictions` and `model2_predictions`.
        - Scatter plots of actual vs. predicted salaries.
        - Potentially, show sample predictions.
4.  **Build Dashboard:** Assemble the charts and descriptive text elements into a cohesive dashboard using Superset's layout elements (Tabs, Rows, Columns, Headers, Text, Dividers).
5.  **Publish Dashboard:** Make the dashboard publicly accessible.

## Conclusion
Stage IV successfully prepared the ground for the final presentation layer. It created the necessary Hive table structures to expose Stage III's modeling results and calculated KL divergence as an additional model comparison metric. The automation script (`scripts/stage4.sh`) handles the prerequisite steps for dashboard creation. The final step involves manually building and publishing the dashboard in Apache Superset using the prepared Hive and PostgreSQL data sources. 