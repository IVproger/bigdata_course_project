import psycopg2 as psql
from pprint import pprint
import os
import re


# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
        password=file.read().rstrip()

# build connection string
conn_string="host=hadoop-04.uni.innopolis.ru port=5432 user=team14 dbname=team14_projectdb password={}".format(password)

# Connect to the remote dbms
with psql.connect(conn_string) as conn:
        
        # Create a cursor for executing psql commands
        cur = conn.cursor()
        # Read the commands from the file and execute them.
        print("Create tables")
        with open(os.path.join("sql","create_tables.sql")) as file:
                content = file.read()
                cur.execute(content)
        conn.commit()
        
        # Handle import_data.sql with COPY commands
        print("Import data")
        with open(os.path.join("sql", "import_data.sql")) as file:
           content = file.read()
        
           # Find all COPY commands and their corresponding table names
           copy_commands = re.findall(r'COPY\s+(\w+)', content, re.IGNORECASE)
        
           for table_name in copy_commands:
            # Extract the specific COPY command for this table
            copy_pattern = rf'COPY\s+{table_name}\s+.*?;'
            match = re.search(copy_pattern, content, re.IGNORECASE | re.DOTALL)
            if match:
                copy_command = match.group(0)
                # Use the table name to get the correct CSV file
                csv_file = os.path.join("data", f"{table_name}.csv")
                if os.path.exists(csv_file):
                    with open(csv_file, "r") as data_file:
                        # Use copy_expert to execute the COPY command
                        cur.copy_expert(copy_command, data_file)
                else:
                    print(f"Warning: CSV file {csv_file} not found")

        conn.commit() 
        
        # Execute test queries
        print("Running test queries")
        cur = conn.cursor()
        with open(os.path.join("sql", "test_database.sql")) as file:
                # Combine all lines into a single string
                sql_content = file.read()
                
                # Split by semicolons to separate commands
                sql_commands = sql_content.split(';')
                
                for command in sql_commands:
                    command = command.strip()

                    # Remove all standalone comment lines
                    cleaned_command = '\n'.join([line for line in command.split('\n')])
                    if cleaned_command.strip():
                        try:
                           cur.execute(cleaned_command)
                           
                           # Fetch and print results if available
                           if cur.description:
                                    pprint(cur.fetchall())
                        except psql.Error as e:
                                print(f"Error executing: {cleaned_command}")
                                print(f"Error message: {e}")