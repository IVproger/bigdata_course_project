Stage I - Data collection and Ingestion
Course: Big Data - IU S25
Author: Firas Jolha

Dataset
Some emps and depts
https://disk.yandex.com/d/EgOiWzvrh9qLIg
Agenda
Stage I - Data collection and Ingestion
Dataset
Agenda
Prerequisites
Objectives
Description
Dataset Description
IU Hadoop cluster
Initiate the project repository
Download dataset
Build a relational database via PostgreSQL
Import the database into HDFS via Sqoop
AVRO
Parquet
Row vs Columnar Format
Snappy
References
Prerequisites
You have access to IU Hadoop cluster
Objectives
Analyze the business problem
Build databases for data storage
Ingest the data into HDFS
Description
Big data is changing the way companies do business and creating a need for data engineers who can collect and manage large quantities of data. Data engineering is the practice of designing and building systems for collecting, storing, and analyzing data at scale.

In the final project of this course, you need to build an end-to-end big data pipeline that accepts input data from some data source, analyzes the data in the cluster and displays the results in a dashboard. We divided the project into four stages where in this tutorial we will cover the first stage in simple manner. Before starting the first stage, it is always good to check the data and perform cleaning/preprocessing if needed. For simplicity, here, we are dealing with batch processing since the data in the files are fixed and no real-time updates exist. If you prefer to process real-time data, you are welcome. In this first stage, I will present here the steps for building a database and ingesting it into HDFS:
1- Build a relational database via PostgreSQL
2- Import the database into HDFS via Sqoop

Dataset Description
The dataset that I am using in these tutorials is about the departments and employees in a company. It consists of two .csv files.

The file emps.csv contains information about employees:

EMPNO is a unique employee number; it is the primary key of the employee table.
ENAME stores the employee’s name.
The JOB attribute stores the name of the job the employee does.
The MGR attribute contains the employee number of the employee who manages that employee. If the employee has no manager, then the MGR column for that employee is left set to null.
The HIREDATE column stores the date on which the employee joined the company.
The SAL column contains the details of employee salaries.
The COMM attribute stores values of commission paid to employees. Not all employees receive commission, in which case the COMM field is set to null.
The DEPTNO column stores the department number of the department in which each employee is based. This data item acts a foreign key, linking the employee details stored in the EMP table with the details of departments in which employees work, which are stored in the DEPT table.
The file depts.csv contains information about departments:

DEPTNO: The primary key containing the department numbers used to identify each department.
DNAME: The name of each department.
LOC: The location where each department is based.
IU Hadoop cluster
Note: We assume that your current working directory . is your repo root directory (for instance, my repo root directory is /home/teamx/project/my-big-data-project-2025 in the cluster).

Initiate the project repository
Access the cluster using the credentials provided by the TA and go to your home directory which is /home/teamx (where x is your team number). Duplicate the project repo shared on Github. You can learn how to duplicate a repo from this link. The root directory of my repo in the cluster is /home/teamx/project/bigdata-final-project.

Download dataset
You need to write a script to download the dataset files of your project from their sources and put them in a folder like ./data folder. Notice that you need to automate this task.

For instance, I created the following script scripts/data_collection.sh

url="https://disk.yandex.com/d/EgOiWzvrh9qLIg"

wget "$(yadisk-direct $url)" -O data/data.zip

unzip data/data.zip -d data/
cp data/toy-data/*.csv data/
rm -rf data/toy-data
rm data/data.zip
To test data collection step, we can run the script bash scripts/data_collection.sh

You have to run the scripts from the root directory of the local repository and this is how the TA will test your project. So, make sure that you have configured the scripts according to this rule.

Build a relational database via PostgreSQL
In this task, I will use PostgreSQL as data storage and write SQL statements for importing the data files into the database. You can explore the database in an interactive mode in pgAdmin but for project purposes, you need to write them in .sql files and then you execute the statements using Python scripts. Here, I will use psycopg2 library for python 3. I will create three sql files (1.sql/create_tables.sql and 2. sql/import_data.sql 3.sql/test_database.sql) then I will write a Python script scripts/build_projectdb.py which will use execute and copy_expert functions from the abovementioned library to create the tables and import the data. This library uses The JDBC driver for Postgresql to run SQL statements on the dbms. You can also connect to the database (JDBC connection) using the tool beeline from Apache Hive (This is made to run HiveQL queries which will be covered in stage II). Notice that the script scripts/build_projectdb.py should replicate this task without errors. This means, for the second run of the script I should not get any errors.

I will give here some of the basic steps to build this database teamx_projectdb but feel free to extend it for your project purposes.

Note: You do not need to drop or create a database since it is already created by the TA.

We will store the SQL statements for building the tables in a file sql/create_tables.sql.

Create tables emps, depts.

-- Optional
-- but it is useful if you want to not commit the change when some errors happened before the commit statement
START TRANSACTION;

DROP TABLE emps CASCADE;
DROP TABLE depts CASCADE;

-- Add tables
-- emps table
CREATE TABLE IF NOT EXISTS emps (
    empno integer NOT NULL PRIMARY KEY,
    ename VARCHAR ( 50 ) NOT NULL,
    job VARCHAR ( 50 ) NOT NULL,
    mgr integer,
    hiredate date,
    sal decimal(10, 2),
    comm decimal(10, 2),
    deptno integer NOT NULL
);

-- dept table
CREATE TABLE IF NOT EXISTS depts(
    deptno integer NOT NULL PRIMARY KEY,
    dname varchar(50) NOT NULL,
    location varchar(50) NOT NULL
);
Note: In the standard, it is not necessary to issue START TRANSACTION to start a transaction block: any SQL command implicitly begins a block. PostgreSQL’s behavior can be seen as implicitly issuing a COMMIT after each command that does not follow START TRANSACTION (or BEGIN), and it is therefore often called “autocommit”. Other relational database systems might offer an autocommit feature as a convenience.

Add the constraints.
-- Add constraints
-- FKs
ALTER TABLE emps DROP CONSTRAINT IF EXISTS fk_emps_mgr_empno;

ALTER TABLE emps ADD CONSTRAINT fk_emps_mgr_empno FOREIGN KEY(mgr) REFERENCES emps (empno);

ALTER TABLE emps DROP CONSTRAINT IF EXISTS fk_emps_deptno_deptno;

ALTER TABLE emps ADD CONSTRAINT fk_emps_deptno_deptno FOREIGN KEY(deptno) REFERENCES depts (deptno);

-- This is related to how the date types are stored in my dataset
ALTER DATABASE team0_projectdb SET datestyle TO iso, ymd;

COMMIT;
Load data from csv files. Here I will create another sql file sql/import_data.sql.
-- always test if you can import the data from PgAdmin then you automate it by writing the script
COPY depts FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';

COPY emps FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';

Perform select query on the database to check the results. At the end commit the transaction block. We can put these sql statements in sql/test_database.sql.
-- optional
-- For checking the content of tables
SELECT * FROM emps LIMIT 10;
SELECT * FROM depts LIMIT 10;

You can also use beeline to connect to the database as follows:

You add the JDBC driver for Postgresql
You connect by passing JDBC connection string
You insert your username and password
#> beeline

beeline> !addlocaldriverjar /shared/postgresql-42.6.1.jar
scan complete in 734ms
beeline> !connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb
scan complete in 0ms
scan complete in 399ms
Connecting to jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb
Enter username for jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb: teamx
Enter password for jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb: **********
Connected to: PostgreSQL (version 14.3)
Driver: PostgreSQL JDBC Driver (version 42.6.1)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:postgresql://hadoop-04.uni.innopolis.> -- execute SQL commands here
In beeline, you cannot run psql meta-commands.

Note: Do not forget, that the tasks in the project should not return errors when I run the stages for the second time, so you should clear/drop the objects before creating new ones.

Note: The max size to load data from PgAdmin is 200MB.

Now, we can interact with the database using the Python package psycopg2.

You can install the psycopg2 package as follows:

pip install psycopg2-binary
Before writing any Python scripts, create a virtual environment in your local directory using any tool you prefer (e.g. venv).

You can a virtual environment in Python as follows:

python -m venv venv

source ./venv/bin/activate
The script scripts/build_projectdb.py will run all SQL scripts abovementioned.

import psycopg2 as psql
from pprint import pprint
import os


# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
        password=file.read().rstrip()

        

# build connection string
conn_string="host=hadoop-04.uni.innopolis.ru port=5432 user=teamx dbname=teamx_projectdb password={}".format(pass)


# Connect to the remote dbms
with psql.connect(conn_string) as conn:
        
        # Create a cursor for executing psql commands
        cur = conn.cursor()
        # Read the commands from the file and execute them.
        with open(os.path.join("sql","create_tables.sql")) as file:
                content = file.read()
                cur.execute(content)
        conn.commit()

        # Read the commands from the file and execute them.
        with open(os.path.join("sql", "import_data.sql")) as file:
                # We assume that the COPY commands in the file are ordered (1.depts, 2.emps)
                commands= file.readlines()
                with open(os.path.join("data","depts.csv"), "r") as depts:
                        cur.copy_expert(commands[0], depts)
                with open(os.path.join("data","emps.csv"), "r") as emps:
                        cur.copy_expert(commands[1], emps)

        # If the sql statements are CRUD then you need to commit the change
        conn.commit()

        pprint(conn)
        cur = conn.cursor()
        # Read the sql commands from the file
        with open(os.path.join("sql", "test_database.sql")) as file:
                commands = file.readlines()
                for command in commands:
                        cur.execute(command)
                        # Read all records and print them                        
                        pprint(cur.fetchall())

We add the command python scripts/build_projectdb.py to run this script to scripts/data_storage.sh file.

Import the database into HDFS via Sqoop
Sqoop is a command-line tool used for importing data from relational databases to HDFS. It is a data ingestion tool that enables you to bulk import and export data from a relational database to/from HDFS. You can use Sqoop to import data into HDFS or directly into Hive. Sqoop is installed in the cluster as a client and we will use the command sqoop for that purpose. We will import the tables created in PostgreSQL to HDFS and store them in /user/teamx/project/warehouse folder.

Now we are ready to run commands on Sqoop to call Postgresql databases. You run these commands on the cluster node hadoop-01.uni.innopolis.ru where you have access in. Here, I put some examples.

You can read the password from secrets/.psql.pass in the terminal as follows:

password=$(head -n 1 secrets/.psql.pass)
List all databases for user teamx
sqoop list-databases --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb --username teamx --password $password
List all tables of database teamx_projectdb for user teamx
sqoop list-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb --username teamx --password $password
Import all tables of the database emps and store them in HDFS at /user/teamx/project/warehouse folder. The datafiles will be compressed in Snappy and serialized in AVRO format.
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb --username teamx --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1
Make sure that the folder project/warehouse is empty in HDFS and this command will generate .avsc and .java files for each table. Keep them since they will be used in the next stage. You can put them in a folder /output in your repository.

Import a specific table from the database emps with specific columns empno, sal and store it in HDFS at /user/teamx/project/warehouse folder
sqoop import --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb --username teamx --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1 --table emps --columns empno, sal
When importing data in Sqoop, you could run a SQL query using --query option and also filter data using --where option. More options in the documentation.

You can use the option --direct if you have psql installed which does not create Map-reduce jobs (to run on Hadoop Map-reduce engine) and the task is executed using psql command. Here, we do not use --direct option, such that we use Mapreduce engine for running Sqoop jobs.

Note: Do not forget to clear the folder hdfs://user/teamx/project/warehouse if exists before importing the data.

For the project, you need to import the data from Postgresql to HDFS using a big data file format (e.g. AVRO --as-avrodatafile, or Parquet --as-parquetfile) and compressed (e.g. in gzip --compression-codec=gzip (default), Snappy --compression-codec=snappy, or Bzip --compression-codec=bzip2).

Important: For the project, you should select one compression method and one file format that give you more efficient storage saving and fast data retrieval. Add the justification for your selection to the report.

AVRO
Avro is a row oriented semi-structured data format for storing Big Data files, actively used in the Apache Hadoop ecosystem and widely used as a serialization platform. In addition to the semi-structured nature of the Avro format, the files are also splittable, which means that the Hadoop platform can separate the file into individual sections which increases the processing efficiency during data analysis.



Avro stores the data definition as schemas in JSON format .avsc, making it easy to read and interpret; the data itself is stored in binary format .avro making it compact and efficient. Avro files include markers that can be used to split large data sets into subsets suitable for Apache MapReduce processing. When Avro data is read, the schema used when writing it is always present. This also facilitates use with dynamic, scripting languages, since data, together with its schema, is fully self-describing.



Parquet
Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. Apache Parquet is designed to be a common interchange format for both batch and interactive workloads.

The following table compares the savings as well as the speedup obtained by converting data into Parquet from CSV.


Row vs Columnar Format
Both file formats are a great choice for your ETL pipelines. However, the decision to incorporate one or the other into your data integration ecosystem will be guided by your business requirements.


Avro and Parquet are both popular big data file formats that are well-supported. The comparison between big data file formats are shown in the table below.


For instance, Avro is a better option if your requirements entail writing vast amounts of data without retrieving it frequently. This is because Avro is a row-based storage file format, and these types of file formats deliver the best performance with write-heavy transactional workloads.

On the other hand, if your use case requires you to analyze petabytes of data without sacrificing performance, then a column-based file format, such as Parquet, is your best bet since column-based file formats naturally deliver unparalleled performance with read-heavy analytical querying of data.

As far as ETL is concerned, you will find that Avro outperforms Parquet. This is because Parquet is designed for querying only subsets of specific columns instead of querying entire data from all the columns.

Snappy
Snappy is a compression/decompression library. It optimizes for very high-speed compression and decompression. It does not aim for maximum compression, or compatibility with any other compression library; instead, it aims for very high speeds and reasonable compression.



As shown in the chart above that Avro increases storage space and write time modestly while significantly reducing read time. The addition of Snappy compression increases write time minimally, while significantly decreasing storage space and maintaining minimal read time. The resulting combination optimizes for single archival write with multiple read usage. You can check the paper from here.

References
toy-dataset
Sqoop User Guide
PostgreSQL documentation
Apache Parquet
Apache Avro
Apache ORC
A Comparative Analysis of Avro, Parquet, and ORC: Understanding the Differences
How to choose between Parquet, ORC and AVRO for S3, Redshift and Snowflake?
What is Parquet