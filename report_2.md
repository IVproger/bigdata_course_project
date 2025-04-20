# Stage II - Data Storage/Preparation & EDA Report

## Overview
This report details the implementation of Stage II of the big data pipeline project, which focused on data storage preparation and exploratory data analysis (EDA). The project involved creating a Hive table using AVRO format with partitioning and bucketing optimizations, and performing EDA on the job descriptions dataset using Spark SQL, storing results in PostgreSQL, and exporting them to local CSV files.

## Dataset Description
The dataset consists of job descriptions from various companies, containing detailed information about job postings. This is the same dataset used in Stage I, which was imported into HDFS using Sqoop and stored as AVRO files.

## Implementation Steps

### 1. Hive Database and Table Creation (`sql/db.hql`)
- Created a Hive database `team14_projectdb` located at `project/hive/warehouse`.
- Created an initial external Hive table `job_descriptions` pointing to the AVRO data imported in Stage 1 (`project/warehouse/job_descriptions`).
- Created the primary analysis table `job_descriptions_part` as an external, partitioned, and bucketed table:
  - **Partitioned by:** `work_type` (STRING)
  - **Bucketed by:** `preference` (STRING) into 3 buckets.
  - **Storage Format:** AVRO with SNAPPY compression.
  - **Location:** `project/hive/warehouse/job_descriptions_part`
- Used appropriate data types, including `DECIMAL(9,6)` for latitude/longitude and `DATE` for `job_posting_date` (converted from Unix timestamp in milliseconds using `FROM_UNIXTIME` and `CAST`).
- Ensured Hive settings `hive.exec.dynamic.partition=true`, `hive.exec.dynamic.partition.mode=nonstrict`, and `hive.enforce.bucketing = true` were enabled within `sql/db.hql`.

### 2. Data Loading
- Populated the `job_descriptions_part` table using an `INSERT INTO ... SELECT ...` statement from the initial `job_descriptions` table.
- This insert operation dynamically created partitions based on the `work_type` column.
- The original `job_descriptions` table was then dropped as specified in the task requirements (`DROP TABLE job_descriptions;`).

### 3. Exploratory Data Analysis (EDA) (`sql/q1.hql` - `sql/q6.hql` & `scripts/run_hive_queries.py`)
- Six key analyses were performed on the `job_descriptions_part` table using Spark SQL, executed via the `scripts/run_hive_queries.py` script, which reads the HQL files.

1.  **Average Salary by Country (`q1.hql`)**
    - Calculated the average salary (midpoint of `salary_range` after parsing 'K' suffix) for each country.
    - Filtered to include only countries with more than 10 job postings.
    - Results ordered by average salary descending.
2.  **Top Roles by Gender Preference (`q2.hql`)**
    - Determined the top 10 most frequent roles for each gender preference category ('Female', 'Male', 'Both') based on keywords in the `preference` column.
    - Results ordered by gender and role count.
3.  **Monthly Job Posting Trend (`q3.hql`)**
    - Aggregated the count of job postings by month and year.
    - Extracted month-year using `FROM_UNIXTIME` and `UNIX_TIMESTAMP` on the `job_posting_date`.
    - Results ordered by month.
4.  **Job Categories by Count and Average Salary (`q4.hql`)**
    - Categorized jobs based on keywords in the `job_title` (e.g., 'Technology', 'Data & Analytics', 'Healthcare').
    - Calculated the total job count and average salary for each category.
    - Results ordered by job count descending.
5.  **Top Job Titles and Most Common Qualification (`q5.hql`)**
    - Identified the top 10 job titles by posting count.
    - Found the most frequently listed qualification for each of those top 10 job titles.
    - Results ordered by posting count descending.
6.  **Bi-Annual Trend for Top Roles (`q6.hql`)**
    - Tracked the number of job postings for the top 5 most frequent roles (excluding 'Other') across bi-annual periods (H1/H2).
    - Calculated the percentage of each role's total postings that occurred in each half-year.
    - Results ordered by role's total jobs and then by half-year.

### 4. Result Storage and Export (`scripts/run_hive_queries.py`)
- **PostgreSQL Storage:** The results of each EDA query (`q1`-`q6`) were written to corresponding tables (`q1_results`, `q2_results`, ..., `q6_results`) in the PostgreSQL database `team14_projectdb` on `hadoop-04.uni.innopolis.ru`.
  - This was achieved using Spark's JDBC data source capabilities within the `run_hive_queries.py` script, using `overwrite` mode.
- **CSV Export:** The data from each PostgreSQL results table was then read back into a Spark DataFrame and written to a local CSV file (`output/q1.csv`, `output/q2.csv`, ..., `output/q6.csv`) with headers.
  - This was done using `pg_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_csv_path)` in the Python script to ensure a single CSV file per query result.

### 5. Automation (`scripts/stage2.sh`, `scripts/create_hive_tables.sh`)
- A shell script (`scripts/stage2.sh`) was created to automate the main tasks of Stage 2.
- The script first executes `scripts/create_hive_tables.sh`, which runs the `sql/db.hql` script using `beeline` to set up the Hive database and the final `job_descriptions_part` table. The output of `beeline` is redirected to `output/hive_results.txt`.
- `stage2.sh` then sets the `YARN_CONF_DIR` environment variable.
- It uses `spark-submit` to execute the `scripts/run_hive_queries.py` script on the YARN cluster, which performs the EDA, stores results in PostgreSQL, and exports them to CSV.
- Finally, `stage2.sh` runs `pylint` to check the quality of the Python script.
- Note: Creation of visualizations in Apache Superset from the PostgreSQL tables and exporting them as images (`output/q*.jpg`) is a manual step, as indicated in the task description and the `stage2.sh` output notes.

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
  - Reading data back from PostgreSQL and using Spark's `coalesce(1).write.csv` provided a robust way to generate single, headered CSV files locally via the `run_hive_queries.py` script.

## Results and Verification
- Successfully created the Hive database `team14_projectdb` and the partitioned/bucketed table `job_descriptions_part`.
- Verified data integrity and structure through Hive `SHOW TABLES;`, `SHOW PARTITIONS job_descriptions_part;`, and sample queries within `sql/db.hql`.
- Successfully executed the 6 EDA queries using `scripts/run_hive_queries.py`.
- Verified the creation and population of PostgreSQL tables `q1_results` through `q6_results`.
- Verified the generation of local CSV files `output/q1.csv` through `output/q6.csv` containing the query results with headers.
- Verified the creation of `output/hive_results.txt` containing the output from the Hive table setup script.
- Confirmed the existence of chart images (`output/q1.jpg` - `output/q6.jpg`), acknowledging their manual generation process via Apache Superset.
- The `scripts/stage2.sh` script successfully automates the execution of the Hive table creation and EDA process.
- Pylint check completed as part of `stage2.sh`.

## Conclusion
Stage II successfully transitioned from the initial data ingestion to creating an optimized data warehouse structure in Hive and performing exploratory data analysis. The `job_descriptions_part` table, partitioned by `work_type` and bucketed by `preference` using the AVRO format, serves as the foundation for analysis. The EDA process, automated via `scripts/stage2.sh` (which orchestrates `scripts/create_hive_tables.sh` and `scripts/run_hive_queries.py`), leverages Spark SQL to execute 6 distinct HiveQL queries. The results are stored efficiently in PostgreSQL for potential visualization in tools like Superset, and exported to local CSV files for accessibility. This stage provides valuable insights into the job market data and sets the stage for further analysis and dashboard creation.
