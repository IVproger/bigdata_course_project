# Big Data Final Project Report - Team 14

## Title Page

**Project Title:** End-to-End Big Data Pipeline for Job Description Analysis and Salary Prediction  
**Team:** Team 14  
**Course:** Big Data - IU S24  
**Date:** [Insert Date]

---

## Introduction

This report details the implementation of the Big Data final project for Team 14. The project encompasses the design, development, and execution of a comprehensive data pipeline aimed at processing, analyzing, and modeling job description data obtained from Kaggle. The pipeline integrates various big data technologies to handle tasks ranging from initial data collection and storage to advanced machine learning and preparation for data visualization.

The core objective was to build a scalable and automated system demonstrating proficiency in data ingestion, warehousing, exploration, and predictive modeling within the Hadoop ecosystem. Key technologies employed include PostgreSQL, HDFS, Sqoop, Avro, Snappy, Hive, Spark (SQL, PySpark, MLlib), Bash, and Python. The pipeline culminates in preparing analytical results and model outputs for presentation, potentially via a dashboarding tool like Apache Superset.

This document outlines the business objectives, describes the dataset and data characteristics, details the pipeline architecture and implementation across its four stages, presents the results of data analysis and machine learning, and concludes with reflections and recommendations.

---

## Business Objectives

The primary business objectives driving this project were:

1.  **Understand Job Market Trends:** Analyze job posting data to identify patterns related to salary, required qualifications, popular roles, geographic distribution, and temporal trends.
2.  **Predict Job Salaries:** Develop machine learning models to predict the average salary for a job posting based on its characteristics (e.g., title, required skills, experience, location).
3.  **Develop a Scalable Data Processing Framework:** Implement an automated pipeline capable of handling potentially large volumes of job data, demonstrating best practices in data storage, processing, and analysis using big data tools.
4.  **Provide Actionable Insights:** Prepare the findings from EDA and ML modeling in a format suitable for presentation and potential use in business decision-making (e.g., recruitment strategy, salary benchmarking).

---

## Data Description

The project utilizes the "Job Description Dataset" sourced from Kaggle (`ravindrasinghrana/job-description-dataset`). This dataset is a comprehensive collection of **synthetic job postings**, created specifically for facilitating research and analysis in areas such as job market trends, Natural Language Processing (NLP), and Machine Learning model development within an educational context.

It offers a diverse set of job listings across various industries and job types. The columns included in the dataset are:

*   **Job Id:** A unique identifier for each job posting.
*   **Experience:** The required or preferred years of experience.
*   **Qualifications:** The educational qualifications needed.
*   **Salary Range:** The range of salaries or compensation offered.
*   **Location:** The city or area where the job is located.
*   **Country:** The country where the job is located.
*   **Latitude:** The latitude coordinate of the job location.
*   **Longitude:** The longitude coordinate of the job location.
*   **Work Type:** The type of employment (e.g., Full-time, Part-time, Contract).
*   **Company Size:** The approximate size or scale of the hiring company.
*   **Job Posting Date:** The date when the job posting was made public.
*   **Preference:** Special preferences or requirements for applicants (e.g., "Only Male", "Only Female", "Both").
*   **Contact Person:** The name of the contact person or recruiter.
*   **Contact:** Contact information for job inquiries.
*   **Job Title:** The job title or position being advertised.
*   **Role:** The role or category of the job (e.g., Software Developer, Marketing Manager).
*   **Job Portal:** The platform or website where the job was posted.
*   **Job Description:** A detailed description of the job responsibilities and requirements.
*   **Benefits:** Information about benefits offered (e.g., health insurance, retirement plans).
*   **Skills:** The skills or qualifications required.
*   **Responsibilities:** Specific responsibilities and duties associated with the job.
*   **Company Name:** The name of the hiring company.
*   **Company Profile:** A brief overview of the company's background and mission.

This rich, albeit synthetic, dataset serves as the input for all subsequent processing and analysis stages in the project. Its structure allows for exploration into various potential use cases, such as building predictive models for job market trends, enhancing job recommendation systems, developing NLP models for resume parsing, analyzing regional job market disparities, and exploring salary prediction models.

*(Acknowledgements from the dataset creators: Python Faker library for dataset generation and ChatGPT for fine-tuning and quality assurance.)*

---

## Data Characteristics

*(This section should ideally be populated with findings from initial data exploration, potentially performed before or during Stage 1/2. Examples include:)*

*   **Volume:** Total number of records (job postings) in the dataset. (e.g., obtained via `SELECT COUNT(*)` in `sql/test_database.sql`).
*   **Data Types:** Mix of numerical (latitude, longitude, company size), categorical (work type, preference, country), date/time (posting date), and text data (description, skills, qualifications, title).
*   **Missing Values:** Assessment of missing values in key columns (e.g., salary range, experience, qualifications). *(Mention if any specific handling like imputation or removal was done, although the reports suggest dropping rows with nulls was the primary method in Stage 3 preprocessing)*.
*   **Distribution:** Brief notes on the distribution of important features (e.g., salary ranges are often skewed, distribution of job postings across countries/work types).
*   **Text Data:** High cardinality and unstructured nature of text fields like Job Description, Skills, Responsibilities require NLP techniques for feature extraction.
*   **Coordinates:** Latitude and Longitude present, requiring validation (constraints added in `sql/create_tables.sql`).
*   **Date Range:** Span of the `job_posting_date`.

*(Please add specific observations about the dataset characteristics here based on any initial analysis performed).*

---

## Architecture of Data Pipeline

The project follows a staged batch processing architecture, leveraging various components of the Hadoop ecosystem and related technologies. The diagram below illustrates the flow:

```mermaid
graph LR
    subgraph Input
        Kaggle[(Kaggle API)]
    end

    subgraph Stage 1: Collection & Initial Storage
        direction LR
        LocalCSV[Local CSV File<br>(data/*.csv)]
        Postgres[(PostgreSQL DB<br>team14_projectdb)]
        Sqoop[Sqoop Import]
        HDFS_Avro[HDFS Avro/Snappy<br>(project/warehouse)]
        Kaggle --> |scripts/data_collection.sh| LocalCSV
        LocalCSV --> |scripts/build_projectdb.py| Postgres
        Postgres --> |scripts/stage1.sh| Sqoop
        Sqoop --> HDFS_Avro
    end

    subgraph Stage 2: Warehousing & EDA
        direction LR
        Hive[Hive External Table<br>(Partitioned/Bucketed<br>project/hive/warehouse)]
        SparkSQL_EDA[Spark SQL EDA<br>(scripts/run_hive_queries.py)]
        Postgres_Results[(PostgreSQL<br>q*_results tables)]
        LocalCSV_Results[Local CSV<br>(output/q*.csv)]
        HDFS_Avro --> |scripts/create_hive_tables.sh| Hive
        Hive --> |Spark SQL| SparkSQL_EDA
        SparkSQL_EDA --> Postgres_Results
        SparkSQL_EDA --> LocalCSV_Results
    end

    subgraph Stage 3: Predictive Analytics
        direction LR
        Spark_Preproc[Spark Preprocessing<br>(scripts/data_preprocessing.py)]
        HDFS_Json[HDFS JSON<br>(project/data/train, test)]
        Spark_ML[Spark ML Modeling<br>(scripts/ml_modeling.py)]
        HDFS_Models[HDFS Models<br>(project/models/)]
        HDFS_Preds[HDFS Predictions/Eval<br>(project/output/*.csv)]
        Local_Downloads[Local Files<br>(models/, data/, output/)]

        Hive --> |PySpark| Spark_Preproc
        Spark_Preproc --> HDFS_Json
        HDFS_Json --> |PySpark| Spark_ML
        Spark_ML --> HDFS_Models
        Spark_ML --> HDFS_Preds
        HDFS_Models --> |scripts/stage3.sh| Local_Downloads
        HDFS_Preds --> |scripts/stage3.sh| Local_Downloads
        HDFS_Json --> |scripts/stage3.sh| Local_Downloads
    end

    subgraph Stage 4: Presentation Prep
        direction LR
        Spark_KL[Spark KL Calc<br>(scripts/calculate_kl.py)]
        HDFS_KL[HDFS KL Results<br>(project/output/kl_divergence.csv)]
        Hive_Results_Tables[Hive External Tables<br>for Results]
        Local_KL[Local CSV<br>(output/kl_divergence.csv)]

        HDFS_Preds --> |PySpark| Spark_KL
        Spark_KL --> HDFS_KL
        HDFS_Preds --> |scripts/stage4.sh| Hive_Results_Tables
        HDFS_Eval --> |scripts/stage4.sh| Hive_Results_Tables(HDFS_Preds refers also to HDFS_Eval)
        HDFS_KL --> |scripts/stage4.sh| Hive_Results_Tables
        HDFS_KL --> |scripts/stage4.sh| Local_KL
    end

    subgraph Presentation
       Superset[Apache Superset<br>(Manual Dashboard)]
    end

    Postgres_Results --> Superset
    Hive_Results_Tables --> Superset


    style Input fill:#f9f,stroke:#333,stroke-width:2px
    style Presentation fill:#ccf,stroke:#333,stroke-width:2px
```

**Workflow Summary:**
1.  **Data Acquisition:** Job data is downloaded from Kaggle via its API (`scripts/data_collection.sh`) into a local CSV file.
2.  **Relational Staging:** The CSV data is loaded into a PostgreSQL database (`scripts/build_projectdb.py`) for initial structuring and validation.
3.  **HDFS Ingestion:** Sqoop imports the data from PostgreSQL into HDFS as compressed Avro files (`scripts/stage1.sh`).
4.  **Data Warehousing:** An optimized external Hive table (partitioned and bucketed) is created over the HDFS Avro data (`scripts/create_hive_tables.sh`).
5.  **EDA:** Spark SQL executes HiveQL queries against the Hive table. Results are stored back into PostgreSQL and also exported as local CSV files (`scripts/run_hive_queries.py`).
6.  **ML Preprocessing:** A Spark job reads data (likely from Hive), performs feature engineering and log-transforms the target variable, saving train/test splits as JSON to HDFS (`scripts/data_preprocessing.py`).
7.  **ML Modeling:** Another Spark job loads the preprocessed data from HDFS, trains/tunes models (Linear Regression, GBT), evaluates them, and saves models, predictions (inverse-transformed to original scale), and evaluation metrics to HDFS (`scripts/ml_modeling.py`). Results are downloaded locally.
8.  **Presentation Preparation:** A Spark job calculates KL divergence on the original-scale predictions from HDFS. Hive external tables are created over the HDFS prediction/evaluation/KL CSV files to make them accessible for BI tools (`scripts/stage4.sh`). KL results are downloaded locally.
9.  **Visualization (Manual):** Apache Superset connects to PostgreSQL (for EDA results) and Hive (for ML results via the external tables) to build interactive dashboards.

Automation scripts (`scripts/stage*.sh`) orchestrate the transitions between these stages.

---

## The Input and The Output for Each Stage

**Stage 1: Data Collection and Storage**
*   **Input:** Kaggle API dataset identifier (`ravindrasinghrana/job-description-dataset`), PostgreSQL credentials (`secrets/.psql.pass`).
*   **Output:**
    *   Local raw data CSV: `data/job_descriptions.csv`
    *   Populated PostgreSQL table: `team14_projectdb.job_descriptions`
    *   HDFS data: Snappy-compressed Avro files in `project/warehouse/job_descriptions/`
    *   Local metadata: `output/job_descriptions.avsc`, `output/job_descriptions.java`
    *   HDFS metadata: `project/warehouse/avsc/job_descriptions.avsc`

**Stage 2: Hive Data Warehousing & EDA**
*   **Input:** HDFS Avro data (`project/warehouse/job_descriptions/`), HDFS Avro schema (`project/warehouse/avsc/job_descriptions.avsc`), HiveQL EDA queries (`sql/q*.hql`), PostgreSQL credentials (`secrets/.psql.pass`).
*   **Output:**
    *   Hive table: `team14_projectdb.job_descriptions_part` (external, partitioned, bucketed)
    *   EDA results in PostgreSQL: `team14_projectdb.q*_results` tables
    *   EDA results as local CSV: `output/q*.csv`
    *   Hive script log: `output/hive_results.txt`
    *   (Manual) Visualization charts: `output/q*.jpg`/`png`

**Stage 3: Predictive Data Analytics**
*   **Input:** Data from Hive table `team14_projectdb.job_descriptions_part` (or potentially HDFS Avro data directly, depending on `scripts/data_preprocessing.py` logic).
*   **Output:**
    *   Preprocessed data in HDFS: JSON files in `project/data/train/`, `project/data/test/`
    *   Preprocessed data locally: `data/train.json`, `data/test.json`
    *   Trained models in HDFS: `project/models/model1/`, `project/models/model2/`
    *   Trained models locally: `models/model1/`, `models/model2/`
    *   Predictions/Evaluation in HDFS: CSV files in `project/output/` (e.g., `model*_predictions.csv`, `evaluation.csv`, `*_tuning_results.csv`)
    *   Predictions/Evaluation locally: CSV files in `output/`

**Stage 4: Presentation Preparation**
*   **Input:** Prediction/Evaluation CSVs from HDFS (`project/output/model*_predictions.csv`, `project/output/evaluation.csv`).
*   **Output:**
    *   KL Divergence results in HDFS: `project/output/kl_divergence.csv`
    *   KL Divergence results locally: `output/kl_divergence.csv`
    *   Hive external tables for results: `evaluation_results`, `model1_predictions`, `model2_predictions`, `kl_divergence`
    *   Hive script log: `output/stage4_hive_results.txt`

---

## Data Preparation

Data preparation occurred primarily in two phases: initial loading/structuring in Stage 1 and feature engineering/preprocessing in Stage 3.

### ER Diagram (Conceptual)

The primary relational structure was defined in Stage 1 within PostgreSQL.

*   **Table:** `job_descriptions`
    *   **Columns:** (Includes fields like `id` (PK), `job_id` (unique), `experience`, `qualifications`, `salary_range`, `location`, `country`, `latitude`, `longitude`, `work_type`, `company_size`, `job_posting_date`, `preference`, `contact_person`, `contact`, `job_title`, `role`, `job_portal`, `job_description`, `benefits`, `skills`, `responsibilities`, `company_name`, `company_profile`)
    *   **Relationships:** This project uses a single primary table for the job postings. No explicit relationships to other tables (like separate `company` or `location` tables) were defined in the provided schema (`sql/create_tables.sql`).
    *   **Constraints:** Includes primary key (`id`), data type definitions, and check constraints (e.g., latitude/longitude ranges, positive company size, job posting date not in the future).

*(A visual ER diagram based on `sql/create_tables.sql` could be inserted here if generated using an external tool).*

### Some Samples from the Database

*(This section requires manually querying the PostgreSQL `job_descriptions` table and inserting representative sample rows. Example queries from `sql/test_database.sql` could be used, or simple `SELECT * FROM job_descriptions LIMIT 5;`).*

```sql
-- Placeholder for sample data query result
-- Example: SELECT job_title, country, work_type, salary_range FROM job_descriptions LIMIT 5;
-- (Run this query against the PostgreSQL DB and paste the output here)
```

### Creating Hive Tables and Preparing the Data for Analysis

As detailed in Stage 2, the data ingested into HDFS (Avro format) was structured for efficient analytical querying using Hive:

1.  **Hive Database:** `team14_projectdb` created, located at `project/hive/warehouse`.
2.  **External Table:** The main analysis table `job_descriptions_part` was created as an EXTERNAL table, meaning Hive manages the metadata, but the data resides in the specified HDFS location (`project/hive/warehouse/job_descriptions_part`). Dropping the table does not delete the underlying data.
3.  **Storage Format:** AVRO was used, consistent with the Sqoop import, leveraging Snappy compression.
4.  **Partitioning:** The table was partitioned by `work_type`. This creates separate subdirectories within HDFS for each distinct work type (e.g., Full-time, Part-time). Queries filtering by `work_type` can benefit significantly by only scanning the relevant partitions (subdirectories). Dynamic partitioning (`hive.exec.dynamic.partition.mode=nonstrict`) was enabled during the `INSERT` operation to automatically create partitions based on the data.
5.  **Bucketing:** Within each partition, the data was bucketed by `preference` into 3 buckets. Bucketing physically splits the data within a partition into a fixed number of files based on the hash of the bucketing column (`preference`). This can improve performance for queries involving joins or aggregations on the bucketing column and enables more efficient sampling. `hive.enforce.bucketing = true` was set to ensure data was correctly bucketed during the insert.
6.  **Data Loading:** An `INSERT INTO ... SELECT ...` statement loaded data from the initial temporary table (pointing to Sqoop output) into the final partitioned/bucketed table, performing necessary type conversions (e.g., timestamp to DATE).

This optimized Hive table structure formed the basis for the EDA performed in Stage 2 and potentially the input for ML preprocessing in Stage 3.

---

## Data Analysis

Exploratory Data Analysis (EDA) was performed in Stage 2 using Spark SQL executing HiveQL queries against the optimized `job_descriptions_part` Hive table.

### Analysis Results

The results of the six EDA queries (`sql/q1.hql` - `sql/q6.hql`) were:
1.  Stored in corresponding PostgreSQL tables: `team14_projectdb.q1_results` through `q6_results`.
2.  Exported as local CSV files: `output/q1.csv` through `output/q6.csv`.

These tables/files contain the aggregated data answering the specific analytical questions posed by each query.

*(Refer to the `output/q*.csv` files or query the `q*_results` tables in PostgreSQL for the specific numerical results).*

### Charts

Visualizations for the EDA results were generated manually using Apache Superset, connecting to the PostgreSQL `q*_results` tables. These charts are saved as image files:

*   `output/q1.jpg` / `png`: Visualizing average salary by country.
*   `output/q2.jpg` / `png`: Visualizing top roles by gender preference.
*   `output/q3.jpg` / `png`: Visualizing monthly job posting trends.
*   `output/q4.jpg` / `png`: Visualizing job categories by count and average salary.
*   `output/q5.jpg` / `png`: Visualizing top job titles and their common qualifications.
*   `output/q6.jpg` / `png`: Visualizing bi-annual trends for top roles.

*(Insert or reference the actual chart images here if desired).*

### Interpretation

The EDA aimed to uncover initial insights and patterns within the job market data:
*   **Salary Variations:** Query `q1` explored geographical differences in average salaries.
*   **Role Preferences:** Query `q2` investigated potential associations between job roles and specified applicant preferences.
*   **Temporal Trends:** Query `q3` analyzed seasonality or growth trends in job postings over time.
*   **Category Analysis:** Query `q4` provided a breakdown of job categories based on titles, showing distribution and salary differences.
*   **Skills & Titles:** Query `q5` identified the most in-demand job titles and the qualifications most frequently associated with them.
*   **Role Dynamics:** Query `q6` examined how the prevalence of top roles changed across different halves of the year.

*(Add more specific interpretations based on the actual results observed in the `output/q*.csv` files or charts).*

---

## ML Modeling

Predictive modeling was performed in Stage 3 to predict average job salaries (`salary_avg`) using Spark ML.

### Feature Extraction and Data Preprocessing

The `scripts/data_preprocessing.py` script handled the transformation of raw/Hive data into features suitable for ML models:
1.  **Input:** Data likely read from the `job_descriptions_part` Hive table.
2.  **Feature Engineering:** Specific steps depended on the script logic but generally involved:
    *   Handling categorical features (e.g., `work_type`, `preference`, `country`) using techniques like StringIndexing followed by OneHotEncoding.
    *   Processing numerical features (e.g., `latitude`, `longitude`, potentially derived `experience`). Scaling (e.g., `StandardScaler` or `MinMaxScaler`) might have been applied.
    *   Encoding text features (e.g., `job_title`, `skills`, `job_description`) using methods like TF-IDF or Word2Vec. *(The Stage 3 report mentions specific techniques might have been used, confirm details from the script)*.
3.  **Target Variable Transformation:** The target variable (`salary_avg`, likely derived from `salary_range`) was transformed using `log1p` (natural log of 1 + value). This is crucial for handling the common right-skewness of salary data and improving the performance and stability of regression models, especially linear ones.
4.  **Feature Vector Assembly:** All processed features were combined into a single vector column (typically named "features") using `VectorAssembler`.
5.  **Data Splitting:** The dataset was split into training and testing sets (e.g., 60% train, 40% test).
6.  **Output:** The resulting DataFrames (containing the "features" vector and the log-transformed "label") were saved as JSON files to HDFS (`project/data/train`, `project/data/test`).

### Training and Fine-tuning

The `scripts/ml_modeling.py` script performed the training and tuning:
1.  **Models Selected:**
    *   Linear Regression (`LinearRegression`)
    *   Gradient-Boosted Tree Regressor (`GBTRegressor`)
2.  **Training Data:** Loaded from `project/data/train`.
3.  **Hyperparameter Tuning:**
    *   `ParamGridBuilder` was used to define search spaces for key hyperparameters:
        *   Linear Regression: `regParam`, `elasticNetParam`, `aggregationDepth`.
        *   GBT Regressor: `maxDepth`, `maxIter`, `stepSize`.
    *   `CrossValidator` was employed with **3 folds** to evaluate parameter combinations on the training data.
    *   `RegressionEvaluator` using **RMSE** (Root Mean Squared Error) on the **log-transformed scale** was the metric used to select the best parameters within the cross-validation process.
4.  **Best Model Selection:** The `CrossValidator` identified the hyperparameter set yielding the lowest average RMSE across the folds for each model type. The models trained with these best parameters (`model1` for LR, `model2` for GBT) were saved.

### Evaluation

The performance of the *best* tuned models was evaluated on the held-out test set (`project/data/test`):

1.  **Metrics on Log-Transformed Scale:** Performance was first assessed based on predictions made on the log scale (the scale the models were trained on).
    *   **RMSE (Root Mean Squared Error):** Measures the average magnitude of prediction errors on the log scale.
    *   **R2 (R-squared):** Indicates the proportion of variance in the log-transformed salary explained by the model.
    *   **Results:**
        ```
        +------------------+--------------------+--------------------------+
        | Model_Type       | RMSE (log scale)   | R2 (log scale)           |
        +------------------+--------------------+--------------------------+
        | LinearRegression | 0.0918970521404052 | -1.5970404794174442E-6   |
        | GBTRegressor     | 0.09189212113168939|  1.0571617973409442E-4   |
        +------------------+--------------------+--------------------------+
        ```
    *   **Interpretation:** Both models exhibit very similar RMSE values on the log scale. The R2 values are extremely close to zero (Linear Regression is slightly negative, indicating performance no better than predicting the mean; GBT is slightly positive but still negligible). This suggests that **neither model, with the current feature set and preprocessing, explains a significant amount of the variance in the log-transformed salaries on the test set.**

2.  **Metric on Original Scale (Distribution Comparison):** Kullback-Leibler (KL) Divergence was calculated in Stage 4 to compare the probability distribution of the *original-scale* predicted salaries against the distribution of the *original-scale* actual salaries in the test set. Lower KL divergence indicates the predicted distribution is closer to the actual distribution.
    *   **Results:**
        ```
        +------------------+---------------------+
        | model_type       | kl_divergence       |
        +------------------+---------------------+
        | LinearRegression | 18.77109320336263   |
        | GBTRegressor     | 16.306699360929137  |
        +------------------+---------------------+
        ```
    *   **Interpretation:** The GBT Regressor has a lower KL divergence value than Linear Regression. This suggests that the **distribution of salaries predicted by the GBT model (on the original scale) is slightly closer to the distribution of the actual salaries** compared to the Linear Regression model, even though their point-wise performance (RMSE/R2 on log scale) was very similar and generally poor.

3.  **Prediction Output:** The final predictions for the test set were inverse-transformed (`expm1`) back to the original salary scale and saved alongside the actual labels in `output/model*_predictions.csv`.

---

## Data Presentation

The final stage involved preparing the results for presentation, primarily targeting a dashboarding tool like Apache Superset.

### The Description of the Dashboard

*(This section requires a description of the dashboard created manually in Apache Superset. Placeholder below):*

A dashboard was designed in Apache Superset to provide an interactive overview of the project's findings. It aimed to integrate visualizations from both the Exploratory Data Analysis (Stage 2) and the Machine Learning modeling results (Stage 3 & 4).

The dashboard likely connects to two main data sources:
1.  **PostgreSQL Database (`team14_projectdb`):** Containing the `q*_results` tables generated during EDA in Stage 2.
2.  **Hive Database (`team14_projectdb`):** Accessing the external tables (`evaluation_results`, `model1_predictions`, `model2_predictions`, `kl_divergence`) created in Stage 4, which point to the ML result CSVs stored in HDFS.

The dashboard layout probably includes sections for:
*   EDA Insights (visualizing the `q*` results).
*   ML Model Performance (showing RMSE, R2, KL divergence).
*   Prediction Analysis (e.g., scatter plots of actual vs. predicted salaries, distribution comparisons).

*(Provide a more detailed description based on the actual dashboard built).*

### Description of Each Chart

*(This section requires descriptions of the specific charts included in the manual Superset dashboard. Placeholder below):*

The Superset dashboard contained several charts, including:

*   **EDA Charts (from PostgreSQL `q*_results`):**
    *   Chart 1 (e.g., Bar chart for `q1`): Average Salary by Country. Shows...
    *   Chart 2 (e.g., Grouped bar chart for `q2`): Top Roles by Gender Preference. Illustrates...
    *   Chart 3 (e.g., Line chart for `q3`): Monthly Job Posting Trend. Displays...
    *   Chart 4 (e.g., Combo chart for `q4`): Job Categories by Count and Avg Salary. Compares...
    *   Chart 5 (e.g., Table for `q5`): Top Job Titles and Common Qualifications. Lists...
    *   Chart 6 (e.g., Stacked bar chart for `q6`): Bi-Annual Trend for Top Roles. Shows...
*   **ML Result Charts (from Hive external tables):**
    *   Chart 7 (e.g., Table for `evaluation_results`, `kl_divergence`): Model Performance Metrics (RMSE, R2, KL Divergence). Compares LR vs GBT.
    *   Chart 8 (e.g., Scatter plot from `model1_predictions`): Actual vs. Predicted Salary (Linear Regression - Original Scale). Shows model fit visually.
    *   Chart 9 (e.g., Scatter plot from `model2_predictions`): Actual vs. Predicted Salary (GBT Regressor - Original Scale). Shows model fit visually.
    *   Chart 10 (e.g., Histogram/Density plot from `model*_predictions`): Comparison of Actual vs. Predicted Salary Distributions. Visualizes the KL divergence findings.

*(Replace placeholders with descriptions of the actual charts created in Superset).*

### Your Findings

Based on the EDA and ML modeling stages, key findings include:

*   **EDA Insights:** *(Summarize 2-3 key insights derived from the EDA queries/charts. E.g., "Technology roles were the most frequent category, but Healthcare offered higher average salaries.", "Job postings peaked in Q2 and Q4.", "Significant salary disparities exist between Country X and Country Y.")*
*   **ML Model Performance:** Both the tuned Linear Regression and GBT Regressor models exhibited poor predictive performance on the test set when evaluated using R2 on the log-transformed salary scale (R2 values near zero). This indicates that the chosen features and preprocessing steps were insufficient to capture the variance in job salaries effectively.
*   **Model Comparison:** While point-wise performance (RMSE/R2) was similar, the GBT model showed a slightly better ability to replicate the overall distribution of actual salaries on the original scale, as indicated by its lower KL divergence score compared to Linear Regression.
*   **Data Challenges:** Predicting salaries accurately based solely on the provided features is challenging, likely due to factors like the synthetic nature of the data, the broadness of salary ranges, uncaptured variables influencing salary, and the complexity of text features.

---

## Conclusion

### Summary of the Report

This report documented the successful implementation of an end-to-end big data pipeline (Team 14) for processing job description data. The pipeline involved automated stages for data collection (Kaggle API), storage (PostgreSQL, HDFS Avro/Snappy via Sqoop), optimized warehousing (Hive partitioned/bucketed tables), exploratory analysis (Spark SQL), predictive modeling (Spark ML - LR & GBT with CV tuning, evaluated using RMSE/R2/KL Divergence), and presentation preparation (Hive external tables for Superset). The architecture leveraged key components of the Hadoop ecosystem, and the entire process was orchestrated via Bash and Python scripts. While EDA provided insights into job market characteristics, the ML models developed showed limited predictive power for salaries based on the current features, highlighting the complexity of the prediction task.

### Reflections on Own Work

*(Placeholder for team reflection. Consider aspects like:*
*   *What went well during the project?*
*   *What aspects were learned? (e.g., specific technologies, pipeline design, challenges of ML)*
*   *How effectively did the team collaborate?*
*   *Was the project scope managed effectively?*)

### Challenges and Difficulties

*(Placeholder for challenges faced. Consider aspects like:*
*   *Setting up the environment and tool integrations.*
*   *Debugging distributed jobs (Sqoop, Spark, Hive).*
*   *Handling large datasets or specific data formats (Avro).*
*   *Feature engineering complexities, especially for text data.*
*   *Achieving good ML model performance.*
*   *Understanding and interpreting results from different stages.*
*   *Manual steps like dashboard creation.*)

### Recommendations

*(Placeholder for recommendations. Consider aspects like:*
*   *Further feature engineering (e.g., more advanced NLP on descriptions/skills, extracting years from experience text, interaction terms).*
*   *Exploring different ML models or hyperparameter ranges.*
*   *Investigating reasons for poor model performance (data quality, missing features).*
*   *Improving automation (e.g., automated dashboard generation/updates if possible).*
*   *Using different data partitioning/bucketing strategies in Hive.*
*   *Applying more sophisticated data cleaning or imputation techniques.*)

---

## The Table of Contributions of Each Team Member

*(Placeholder for contribution table. Fill in names and specific contributions for each stage or task).*

| Team Member Name | Contributions                                                                 |
| :--------------- | :---------------------------------------------------------------------------- |
| [Name 1]         | (e.g., Stage 1 implementation, PostgreSQL setup, Sqoop configuration)         |
| [Name 2]         | (e.g., Stage 2 implementation, Hive optimization, Spark SQL EDA queries)      |
| [Name 3]         | (e.g., Stage 3 implementation, Spark ML preprocessing, Model training/tuning) |
| [Name 4]         | (e.g., Stage 4 implementation, KL divergence script, Superset integration prep) |
| [Name ...]       | (e.g., Automation scripts, Documentation, Reporting, Testing)               |

---
