#!/usr/bin/env python3
"""
Script to run Hive EDA queries using PySpark, store results in PostgreSQL,
and export results to CSV.
"""

import os
import sys
from pyspark.sql import SparkSession

def read_password(file_path):
    """Read a password from a file."""
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            return file.read().rstrip()
    except FileNotFoundError:
        print(f"Error: Password file not found at {file_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error reading password file {file_path}: {e}", file=sys.stderr)
        sys.exit(1)

def execute_eda_query(spark, query_num, pg_url, pg_properties):
    """Execute a single EDA Hive query, save to PostgreSQL, and export to CSV."""
    query_file = f"sql/q{query_num}.hql"
    pg_table = f"q{query_num}_results"
    csv_file = f"output/q{query_num}.csv"

    print(f"\nExecuting Query {query_num}: {query_file}")
    try:
        # Read the HQL query
        with open(query_file, "r", encoding='utf-8') as file:
            query = file.read()

        # Remove USE statement if present
        query_lines = query.split('\n')
        cleaned_query = "\n".join(
            line for line in query_lines
            if not line.strip().upper().startswith('USE ')
        )

        # Execute the query using Spark SQL
        print(f"Running query {query_num}...")
        result_df = spark.sql(cleaned_query)
        print(f"Query {query_num} executed successfully. Result preview:")
        result_df.show(5)

        # Write the result to PostgreSQL
        print(f"Writing results of query {query_num} to PostgreSQL table {pg_table}...")
        result_df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", pg_table) \
            .option("user", pg_properties["user"]) \
            .option("password", pg_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote query {query_num} results to PostgreSQL.")

        # Export the result from PostgreSQL to CSV
        print(f"Exporting results of query {query_num} from PostgreSQL to {csv_file}...")
        pg_df = spark.read \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", pg_table) \
            .option("user", pg_properties["user"]) \
            .option("password", pg_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Write to temporary directory
        temp_csv_path = f"{csv_file}.tmp"
        pg_df.coalesce(1).write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(temp_csv_path)

    except Exception as e:
        print(f"Error processing query {query_num} ({query_file}): {e}", file=sys.stderr)
        raise

def main():
    """Main function to run Hive EDA queries."""
    spark = None
    try:
        # Configuration
        warehouse = "project/hive/warehouse"
        hive_metastore_uri = "thrift://hadoop-02.uni.innopolis.ru:9883"
        postgres_jdbc_url = "jdbc:postgresql://hadoop-04.uni.innopolis.ru/team14_projectdb"
        postgres_user = "team14"
        postgres_password_file = "secrets/.psql.pass"
        jdbc_driver_path = "/shared/postgresql-42.6.1.jar"

        # Read Passwords
        postgres_password = read_password(postgres_password_file)

        # Spark Session Creation
        print("Creating Spark session...")
        spark = SparkSession.builder \
            .master("yarn") \
            .appName("Spark SQL Hive EDA to PostgreSQL") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", hive_metastore_uri) \
            .config("spark.sql.warehouse.dir", warehouse) \
            .config("spark.driver.extraClassPath", jdbc_driver_path) \
            .config("spark.jars", jdbc_driver_path) \
            .enableHiveSupport() \
            .getOrCreate()
        print("Spark session created successfully.")

        # Set Database Context
        print("Setting database context to team14_projectdb...")
        spark.sql("USE team14_projectdb;")

        # PostgreSQL Connection Properties
        pg_properties = {
            "user": postgres_user,
            "password": postgres_password,
            "driver": "org.postgresql.Driver"
        }

        # Ensure Output Directory Exists
        os.makedirs("output", exist_ok=True)

        # Execute EDA Queries
        print("Starting EDA query execution...")
        for i in range(1, 7):
            execute_eda_query(spark, i, postgres_jdbc_url, pg_properties)

        print("\nAll EDA queries processed successfully!")

    except Exception as e: 
        print(f"An error occurred in the main script: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if spark:
            print("Stopping Spark session...")
            spark.stop()

if __name__ == "__main__":
    main()
