# Stage I: Data Collection and Storage Implementation Report

## Overview
This report documents the implementation of Stage I of our big data pipeline project, which focuses on data collection, database implementation, and data import to HDFS. The implementation follows a systematic approach to ensure data quality and efficient storage.

## Dataset Description
The project utilizes a job descriptions dataset sourced from Kaggle (ravindrasinghrana/job-description-dataset). The dataset contains detailed information about job postings, including:
- Job details (title, description, requirements)
- Company information (name, profile, size)
- Location data (country, coordinates)
- Employment details (work type, salary range, benefits)
- Skills and qualifications
- Contact information

## Implementation Steps

### 1. Data Collection
The data collection process is automated through a shell script (`scripts/data_collection.sh`) that:
- Creates a local data directory (`./data`).
- Downloads the dataset `ravindrasinghrana/job-description-dataset` from Kaggle using the Kaggle API (`kaggle datasets download`).
- Extracts the dataset to the specified directory.

### 2. Database Implementation
The database implementation is handled by a Python script (`scripts/build_projectdb.py`) using the `psycopg2` library:
- Connects to the `team14_projectdb` PostgreSQL database on `hadoop-04.uni.innopolis.ru` using credentials from `secrets/.psql.pass`.
- Executes the table creation SQL (`sql/create_tables.sql`), ensuring idempotency by using `DROP TABLE ... CASCADE` before `CREATE TABLE`.
- Reads the `COPY` command from `sql/import_data.sql` and uses `cur.copy_expert` to efficiently load data from the local CSV file (`data/job_descriptions.csv`) into the `job_descriptions` table.
- Verifies the implementation by executing test queries defined in `sql/test_database.sql`.

#### Database Schema (`sql/create_tables.sql`)
The `job_descriptions` table includes the following structure:
- Primary key: `id` (SERIAL)
- Unique identifier: `job_id` (BIGINT)
- Text fields for job details, company information, and descriptions
- Numeric fields for coordinates and company size
- Date field for job posting date
- Constraints for data validation:
  - Latitude between -90 and 90
  - Longitude between -180 and 180
  - Positive company size
  - Valid job posting date (not in the future)

### 3. Data Import to HDFS
The data import process is managed by `scripts/stage1.sh`, which orchestrates several steps:
- Loads the PostgreSQL password from `secrets/.psql.pass`.
- Activates the Python virtual environment (`source .venv/bin/activate`).
- Runs the data collection script (`bash scripts/data_collection.sh`).
- Runs the database build script (`python3.11 scripts/build_projectdb.py`).
- Cleans the target HDFS directory (`hdfs dfs -rm -r -f project/*`).
- Imports all tables from the `team14_projectdb` PostgreSQL database to HDFS using the `sqoop import-all-tables` command with the following configurations:
  - **Connection:** `jdbc:postgresql://hadoop-04.uni.innopolis.ru/team14_projectdb`
  - **Format:** Avro data files (`--as-avrodatafile`).
  - **Compression:** Snappy (`--compress --compression-codec=snappy`).
  - **Number of Mappers:** 3 (`--m 3`).
  - **Target HDFS Directory:** `project/warehouse` (`--warehouse-dir=project/warehouse`).
- Moves the generated `.avsc` (Avro schema) and `.java` (Java code for Avro) files from the local directory to the project's `output/` folder.
- Creates an `avsc` subdirectory in HDFS (`project/warehouse/avsc`) and uploads the `.avsc` file there (`hdfs dfs -put output/*.avsc project/warehouse/avsc`), making the schema available for Hive table creation in Stage 2.

## Technical Choices

### File Format: Avro
We chose Avro as the file format for HDFS storage because:
- It provides schema evolution capabilities
- Supports efficient serialization/deserialization
- Maintains data types and structure
- Enables easy integration with various big data tools like Hive and Spark
- As a row-based format, it's suitable for write-heavy operations like the initial Sqoop import

### Compression: Snappy
Snappy compression was selected because:
- It offers a good balance between compression ratio and CPU cost (speed), as highlighted in `task_1.md`
- Provides fast decompression
- Is widely supported in the Hadoop ecosystem
- Reduces storage requirements in HDFS while maintaining reasonably fast query performance for subsequent stages

## Implementation Verification
The implementation includes several verification steps:
1.  Successful execution of `scripts/data_collection.sh` confirms dataset download and extraction
2.  Successful execution of `scripts/build_projectdb.py`, including the execution of test queries from `sql/test_database.sql` (e.g., `SELECT COUNT(*) FROM job_descriptions;`), confirms database creation and data loading
3.  Successful execution of `scripts/stage1.sh` confirms the Sqoop import process
4.  Checking the HDFS directory `project/warehouse` for the `job_descriptions` subdirectory containing Snappy-compressed Avro files (`.avro`)
5.  Verifying the presence of `job_descriptions.avsc` and `job_descriptions.java` in the local `output/` directory
6.  Verifying the presence of `job_descriptions.avsc` in the HDFS directory `project/warehouse/avsc`

## Conclusion
Stage I has been successfully implemented with:
- Automated data collection from Kaggle using the Kaggle API
- A robust PostgreSQL database structure (`job_descriptions` table) with appropriate constraints, populated efficiently using `psycopg2`
- Efficient data import from PostgreSQL to HDFS using `sqoop import-all-tables`, storing data in Avro format with Snappy compression
- Proper handling and storage of Avro schema files for use in subsequent stages
- Comprehensive automation (`stage1.sh`) and verification steps

The implementation provides a solid foundation for the subsequent stages of the big data pipeline project.
