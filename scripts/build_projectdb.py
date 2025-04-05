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
        with open(os.path.join("sql", "import_data.sql")) as file:
           content = file.read()
        
           # Find all COPY commands and their corresponding table names
           copy_commands = re.findall(r'COPY\s+(\w+)', content, re.IGNORECASE)
        
           for table_name in copy_commands:
            # Extract the specific COPY command for this table
            copy_pattern = rf'COPY\s+{table_name}\s+.*?;'
            copy_command = re.search(copy_pattern, content, re.IGNORECASE | re.DOTALL).group(0)
         
            
            csv_file = os.path.join("data", f"{table_name}.csv")
            with open(csv_file, "r") as data_file:
                # Use copy_expert to execute the COPY command
                cur.copy_expert(copy_command, data_file)