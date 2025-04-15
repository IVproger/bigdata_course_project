# Stage II - Data Storage/Preparation & EDA Report

## Overview
This report details the implementation of Stage II of the big data pipeline project, which focused on data storage preparation and exploratory data analysis (EDA). The project involved creating a Hive table using AVRO format with partitioning and bucketing optimizations, and performing EDA on the job descriptions dataset using Spark SQL, storing results in PostgreSQL, and exporting them to local CSV files.

## Dataset Description
The dataset consists of job descriptions from various companies, containing detailed information about job postings. This is the same dataset used in Stage I, which was imported into HDFS using Sqoop and stored as AVRO files.

## Implementation Steps

### 1. Hive Database and Table Creation (`sql/db.hql`)
- Created a Hive database `team14_projectdb` located at `project/hive/warehouse`.
- Created an initial external Hive table `job_descriptions` pointing to the AVRO data imported in Stage 1.
- Created the primary analysis table `job_descriptions_part` as an external, partitioned, and bucketed table:
  - **Partitioned by:** `work_type` (STRING)
  - **Bucketed by:** `preference` (STRING) into 3 buckets.
  - **Storage Format:** AVRO with SNAPPY compression.
  - **Location:** `project/hive/warehouse/job_descriptions_part`
- Used appropriate data types, including `DECIMAL(9,6)` for latitude/longitude and `DATE` for `job_posting_date` (converted from Unix timestamp in milliseconds).
- Ensured Hive settings `hive.exec.dynamic.partition=true`, `hive.exec.dynamic.partition.mode=nonstrict`, and `hive.enforce.bucketing = true` were enabled.

### 2. Data Loading
- Populated the `job_descriptions_part` table using an `INSERT INTO ... SELECT ...` statement from the initial `job_descriptions` table.
- This insert operation dynamically created partitions based on the `work_type` column.
- The original `job_descriptions` table was then dropped.

### 3. Exploratory Data Analysis (EDA) (`sql/q1.hql` - `sql/q5.hql` & `scripts/run_hive_queries.py`)
- Five key analyses were performed on the `job_descriptions_part` table using Spark SQL, executed via the `scripts/run_hive_queries.py` script.

1.  **Job Postings by Country (`q1.hql`)**
    - Analyzed the distribution of job postings by country.
    - Limited to the top 10 countries by job count.
2.  **Job Postings by Work Type (`q2.hql`)**
    - Analyzed the distribution of job postings by work type (e.g., Contract, Full-time).
    - Included percentage calculation for each work type.
3.  **Job Postings by Company Size (`q3.hql`)**
    - Analyzed the distribution of job postings by company size.
    - Created size ranges (1-50, 51-200, 201-1000, 1001-5000, 5000+) using a CASE statement.
4.  **Job Postings by Job Title (`q4.hql`)**
    - Identified the most common job titles.
    - Limited to the top 20 job titles by count.
5.  **Job Postings by Skills (`q5.hql`)**
    - Extracted and analyzed the most frequent skills mentioned in the `skills` column.
    - Used `regexp_replace`, `split`, and `explode` functions to clean and count skills.
    - Limited to the top 30 skills by count.

### 4. Result Storage and Export (`scripts/run_hive_queries.py`)
- **PostgreSQL Storage:** The results of each EDA query (`q1`-`q5`) were written to corresponding tables (`q1_results`, `q2_results`, etc.) in the PostgreSQL database `team14_projectdb` on `hadoop-04.uni.innopolis.ru`.
  - This was achieved using Spark's JDBC data source capabilities.
- **CSV Export:** The data from each PostgreSQL results table was then read back into a Spark DataFrame, collected to the driver node using `.collect()`, and written to a local CSV file (`output/q1.csv`, `output/q2.csv`, etc.) using Python's standard `csv` library.
  - This approach was chosen because the EDA results are expected to be small enough to handle on the driver node, and it avoids complexities with managing HDFS part-files for local export.

### 5. Automation (`scripts/stage2.sh`)
- A shell script (`scripts/stage2.sh`) was created to automate the EDA execution part of Stage 2.
- The script sets the `YARN_CONF_DIR` environment variable.
- It then uses `spark-submit` to execute the `scripts/run_hive_queries.py` script on the YARN cluster.
- Note: The Hive table creation (`sql/db.hql`) is assumed to be done separately or in a preceding step, as `stage2.sh` focuses only on running the EDA queries.

## Technical Choices and Justification

### Partitioning Strategy (`work_type`)
- Partitioning by `work_type` was chosen as it represents a fundamental characteristic of job postings and is likely to be used in filtering for analysis (e.g., analyzing only Full-time roles).
- It provides a moderate number of partitions, improving query performance for analyses filtering by work type.

### Bucketing Strategy (`preference`)
- Bucketing by `preference` (e.g., Gender preference) into 3 buckets was implemented as required by the project description to demonstrate bucketing.
- While `preference` might not be the most optimal key for performance in all scenarios, it fulfills the requirement and can help distribute data somewhat evenly for potential join or aggregation operations involving this column.

### File Format (AVRO)
- AVRO was used as the storage format for the Hive tables, consistent with the output from Stage 1.
- AVRO provides schema evolution support and good integration within the Hadoop ecosystem.
- SNAPPY compression was used for a balance between compression ratio and speed.

### EDA Execution (Spark SQL)
- Spark SQL was chosen for executing the HiveQL EDA queries via the `scripts/run_hive_queries.py` script.
- **Performance:** Spark generally offers better performance than Hive MapReduce, especially for iterative queries or more complex logic (though our queries were relatively simple HiveQL).
- **Integration:** Spark SQL seamlessly reads Hive tables and metadata.
- **Flexibility:** It allowed us to easily integrate the query execution with PostgreSQL interaction (JDBC) and local CSV export (Python `csv`) within a single script.

### Result Storage (PostgreSQL & Local CSV)
- **PostgreSQL:** Storing EDA results in PostgreSQL was chosen as specified in the task requirements for handling intermediate, smaller result sets.
  - It allows easy access for tools like Apache Superset for visualization.
- **Local CSV:** Exporting the final results to local CSV files provides a simple, portable format for sharing or further local analysis.
  - Collecting data to the driver and using Python's `csv` module simplified the export process compared to managing HDFS files for local use.

## Results and Verification
- Successfully created the partitioned and bucketed Hive table `job_descriptions_part`.
- Verified data integrity and structure through sample Hive queries and `DESCRIBE` statements.
- Successfully executed the 5 EDA queries using `scripts/run_hive_queries.py`.
- Verified the creation and population of PostgreSQL tables `q1_results` through `q5_results`.
- Verified the generation of local CSV files `output/q1.csv` through `output/q5.csv` containing the query results with headers.
- The `scripts/stage2.sh` script successfully automates the execution of the EDA process.

## Conclusion
Stage II successfully transitioned from the initial data ingestion to creating an optimized data warehouse structure in Hive and performing exploratory data analysis. The `job_descriptions_part` table, partitioned by `work_type` and bucketed by `preference` using the AVRO format, serves as the foundation for analysis. The EDA process, automated via `scripts/stage2.sh` and `scripts/run_hive_queries.py`, leverages Spark SQL to execute HiveQL queries, stores the results efficiently in PostgreSQL for potential visualization in tools like Superset, and exports them to local CSV files for accessibility. This stage provides valuable insights into the job market data and sets the stage for further analysis and dashboard creation.
