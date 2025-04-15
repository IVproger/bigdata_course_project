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
- Creates a local data directory
- Downloads the dataset from Kaggle using the Kaggle API
- Extracts the dataset to the specified directory

### 2. Database Implementation
The database implementation is handled by a Python script (`scripts/build_projectdb.py`) that:
- Connects to the PostgreSQL database on the remote server
- Creates the necessary table structure using SQL commands from `sql/create_tables.sql`
- Imports data from CSV files using COPY commands defined in `sql/import_data.sql`
- Verifies the implementation through test queries in `sql/test_database.sql`

#### Database Schema
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
The data import process is managed by `scripts/stage1.sh`, which:
- Loads database credentials from a secure location
- Activates the Python virtual environment
- Runs the data collection script
- Builds the project database
- Cleans existing HDFS directories
- Imports data to HDFS using Sqoop with the following configurations:
  - Format: Avro data files
  - Compression: Snappy
  - Number of mappers: 3
  - Warehouse directory: project/warehouse

## Technical Choices

### File Format: Avro
We chose Avro as the file format for HDFS storage because:
- It provides schema evolution capabilities
- Supports efficient serialization/deserialization
- Maintains data types and structure
- Enables easy integration with various big data tools

### Compression: Snappy
Snappy compression was selected because:
- It offers a good balance between compression ratio and speed
- Provides fast decompression
- Is widely supported in the Hadoop ecosystem
- Reduces storage requirements while maintaining query performance

## Implementation Verification
The implementation includes several verification steps:
1. Data collection verification through the Kaggle API
2. Database creation and data import verification through test queries
3. HDFS import verification through directory structure and file checks

## Conclusion
Stage I has been successfully implemented with:
- Automated data collection from Kaggle
- Robust PostgreSQL database structure with appropriate constraints
- Efficient data import to HDFS using Sqoop with Avro format and Snappy compression
- Comprehensive error handling and verification steps

The implementation provides a solid foundation for the subsequent stages of the big data pipeline project.
