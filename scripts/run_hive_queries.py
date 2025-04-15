#!/usr/bin/env python3
"""
Script to run Hive queries using PySpark.
This script reads HiveQL statements from SQL files and executes them using PySpark.
"""

import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, explode, split, regexp_replace, round, expr

def read_password(file_path):
    """Read the Hive password from a file."""
    with open(file_path, "r") as file:
        return file.read().rstrip()

def execute_hive_script(spark, script_path, output_file=None):
    """Execute a HiveQL script using PySpark."""
    try:
        # Read the script file
        with open(script_path, "r") as file:
            script = file.read()
        
        # Split the script into individual statements
        # Use a more robust method to split SQL statements
        statements = []
        current_statement = ""
        
        # Process each line
        for line in script.split('\n'):
            # Skip comments
            if line.strip().startswith('--'):
                continue
                
            # Add the line to the current statement
            current_statement += line + "\n"
            
            # If the line ends with a semicolon, it's the end of a statement
            if line.strip().endswith(';'):
                # Clean up the statement
                clean_stmt = current_statement.strip()
                if clean_stmt:
                    statements.append(clean_stmt)
                current_statement = ""
        
        # Add any remaining statement (without semicolon)
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        # Execute each statement
        for statement in statements:
            print(f"Executing: {statement[:100]}...")
            spark.sql(statement)
        
        # If an output file is specified, save the results
        if output_file and statements:
            # Get the last result set
            result = spark.sql(statements[-1])
            
            # Save to the output file
            result.write.mode("overwrite").csv(output_file, header=True)
            print(f"Results saved to {output_file}")
            
            # Display the results
            result.show()
            
    except Exception as e:
        print(f"Error executing script {script_path}: {e}")
        raise

def export_to_csv(spark, table_name, output_file):
    """Export a Hive table to a CSV file with headers."""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Get the table data
        df = spark.sql(f"SELECT * FROM {table_name}")
        
        # Write to CSV with header
        df.write.mode("overwrite").option("header", "true").csv(output_file)
        
        # Combine all part files into a single CSV
        os.system(f"echo '{','.join(df.columns)}' > {output_file}.csv")
        os.system(f"hdfs dfs -cat {output_file}/* >> {output_file}.csv")
        
        print(f"Results exported to {output_file}.csv")
        
    except Exception as e:
        print(f"Error exporting table {table_name} to CSV: {e}")
        raise

def main():
    """Main function to run Hive queries using PySpark."""
    try:
        # Read the password from secrets file
        password = read_password("secrets/.hive.pass")
        if not password:
            raise ValueError("Hive password is empty")
        
        # Create a Spark session with Hive support
        spark = SparkSession.builder \
            .appName("HiveQueries") \
            .config("spark.sql.warehouse.dir", "project/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # Set the Hive password
        spark.conf.set("hive.password", password)
        
        # Create output directory if it doesn't exist
        os.makedirs("output", exist_ok=True)
        
        # Execute database creation script
        print("Creating Hive database and tables...")
        execute_hive_script(spark, "sql/db.hql", "output/hive_db_results.txt")
        
        # Execute EDA queries
        print("\nExecuting EDA queries...")
        
        # Query 1: Job postings by country
        print("\nQuery 1: Job postings by country")
        execute_hive_script(spark, "sql/q1.hql", "output/q1")
        export_to_csv(spark, "q1_results", "output/q1")
        
        # Query 2: Job postings by work type
        print("\nQuery 2: Job postings by work type")
        execute_hive_script(spark, "sql/q2.hql", "output/q2")
        export_to_csv(spark, "q2_results", "output/q2")
        
        # Query 3: Job postings by company size
        print("\nQuery 3: Job postings by company size")
        execute_hive_script(spark, "sql/q3.hql", "output/q3")
        export_to_csv(spark, "q3_results", "output/q3")
        
        # Query 4: Job postings by job title
        print("\nQuery 4: Job postings by job title")
        execute_hive_script(spark, "sql/q4.hql", "output/q4")
        export_to_csv(spark, "q4_results", "output/q4")
        
        # Query 5: Job postings by skills
        print("\nQuery 5: Job postings by skills")
        execute_hive_script(spark, "sql/q5.hql", "output/q5")
        export_to_csv(spark, "q5_results", "output/q5")
        
        print("\nAll queries executed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        # Stop the Spark session
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 