# Stage II - Data Storage/Preparation & EDA Report

## Overview
This report details the implementation of Stage II of the big data pipeline project, which focused on data storage preparation and exploratory data analysis (EDA). The project involved creating a Hive table with partitioning and bucketing optimizations, and performing EDA on the job descriptions dataset using PySpark.

## Dataset Description
The dataset consists of job descriptions from various companies, containing detailed information about job postings. This is the same dataset used in Stage I, which was imported into HDFS using Sqoop with Parquet format and Snappy compression.

## Implementation Steps

### 1. Hive Database and Table Creation
- Created a Hive database `team14_projectdb` with a single table:
  - `job_descriptions`: External table with partitioning by country and bucketing by job_id

- Database Schema:
  - Used appropriate data types for each field (INT, BIGINT, STRING, DECIMAL, DATE)
  - Implemented partitioning by country for better query performance on country-specific analysis
  - Implemented bucketing by job_id for better query performance on job-specific operations
  - Used Parquet file format with Snappy compression for efficient storage and retrieval

### 2. Hive Optimizations

#### Partitioning
- Implemented partitioning on the `country` column in the `job_descriptions` table
- Benefits:
  - Improved query performance for country-specific analysis
  - Reduced data scanning by only reading relevant partitions
  - Better organization of data by geographical location

#### Bucketing
- Implemented bucketing on the `job_id` column in the `job_descriptions` table
- Created 10 buckets for even distribution of data
- Benefits:
  - Improved query performance for job-specific operations
  - Better data organization for join operations
  - Reduced data scanning for queries filtering on job_id

### 3. Data Loading
- Used dynamic partitioning to load data into the table
- Enabled non-strict mode for dynamic partitioning to allow all partitions to be dynamic

### 4. Exploratory Data Analysis (EDA)
Performed five key analyses on the job descriptions dataset using PySpark:

1. **Job Postings by Country**
   - Analyzed the distribution of job postings by country
   - Limited to top 10 countries for clarity
   - Visualization: Bar chart showing job count by country

2. **Job Postings by Work Type**
   - Analyzed the distribution of job postings by work type (e.g., full-time, part-time, remote)
   - Included percentage calculation for each work type
   - Visualization: Bar chart showing job count and percentage by work type

3. **Job Postings by Company Size**
   - Analyzed the distribution of job postings by company size
   - Created size ranges (1-50, 51-200, 201-1000, 1001-5000, 5000+)
   - Visualization: Bar chart showing job count by company size range

4. **Job Postings by Job Title**
   - Identified the most common job titles
   - Limited to top 20 job titles for clarity
   - Visualization: Horizontal bar chart showing job count by job title

5. **Job Postings by Skills**
   - Extracted and analyzed skills from the skills column
   - Used regex and string functions to clean and split skills
   - Limited to top 30 skills for clarity
   - Visualization: Bar chart showing job count by skill

### 5. Automation
- Created a shell script (`scripts/stage2.sh`) to automate the entire process:
  - Creating Hive database and table
  - Running EDA queries
  - Exporting results to CSV files
- Created a Python script (`scripts/run_hive_queries.py`) to run Hive queries using PySpark
- Implemented proper error handling and logging

## Technical Choices and Justification

### Partitioning Strategy
We chose to partition the data by country for the following reasons:

1. **Query Performance**: Many analyses focus on geographical distribution, making country a natural partitioning key.
2. **Data Distribution**: Countries have a reasonable number of distinct values, avoiding too many small partitions.
3. **Analysis Focus**: Our first EDA query focuses on country-based analysis, benefiting from this partitioning.

### Bucketing Strategy
We chose to bucket the data by job_id for the following reasons:

1. **Query Performance**: Job_id is a unique identifier, ensuring even distribution across buckets.
2. **Join Optimization**: If we need to join with other tables using job_id, bucketing will improve join performance.
3. **Data Organization**: Job_id-based bucketing provides a natural organization for job-specific operations.

### File Format and Compression
We continued to use Parquet with Snappy compression for the following reasons:

1. **Columnar Storage**: Parquet's columnar storage is ideal for analytical queries, which is the primary use case for our EDA.
2. **Compression Efficiency**: Parquet's columnar nature allows for more efficient compression, especially for text fields.
3. **Query Performance**: For our analytical workload, Parquet's columnar format provides better query performance.

### PySpark Implementation
We chose to use PySpark for the following reasons:

1. **Integration**: PySpark provides seamless integration with Hive, allowing us to use both SQL and DataFrame APIs.
2. **Performance**: PySpark's in-memory processing capabilities provide better performance for complex analytical queries.
3. **Flexibility**: PySpark allows us to mix SQL queries with Python code for more complex data processing.
4. **Scalability**: PySpark can handle large datasets efficiently, making it suitable for big data processing.

## Results and Verification
- Successfully created and populated the Hive table
- Verified data integrity through sample queries
- Generated five CSV files with EDA results:
  - `q1.csv`: Job postings by country
  - `q2.csv`: Job postings by work type with percentages
  - `q3.csv`: Job postings by company size range
  - `q4.csv`: Top 20 job titles by count
  - `q5.csv`: Top 30 skills by demand
- Created visualizations for each analysis (to be added to the dashboard in Stage 4)
- Implemented proper error handling and idempotent operations

## Conclusion
The implementation of Stage II successfully established the data warehouse layer of our big data pipeline. We created an optimized Hive table with partitioning by country and bucketing by job_id to improve query performance. We performed comprehensive EDA on the job descriptions dataset using PySpark, extracting valuable insights about job postings across different dimensions.

The combination of partitioning, bucketing, and Parquet with Snappy compression provides an optimal balance between storage efficiency and query performance, particularly suited for our analytical workload. The use of PySpark for data processing and analysis provides better performance and flexibility compared to traditional Hive queries. The EDA results will be valuable for understanding the job market and will be incorporated into the dashboard in Stage 4.
