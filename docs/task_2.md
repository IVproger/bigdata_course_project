Stage II - Data Storage/Preparation & EDA
Course: Big Data - IU S25
Author: Firas Jolha

Dataset
Some emps and depts
Agenda
Stage II - Data Storage/Preparation & EDA
Dataset
Agenda
Prerequisites
Objectives
Introduction to Apache Hive
Dataset Description
Preparation
Build Hive Tables
Hive Optimizations
Partitioning in Hive
Bucketing in Hive
Apache Tez
Perform Exploratory Data Analysis (EDA)
Project Checklist - Stage 2
References
Prerequisites
Stage I is done
The relational database is built.
The database tables are imported to HDFS via Sqoop.
The tables are stored in HDFS serialized and compressed in formats (Snappy-Parquet, Snappy-Avro, …etc).
Objectives
Create Hive tables
Perform EDA using HiveQL
Introduction to Apache Hive
Apache Hive is a distributed, fault-tolerant data warehouse system that enables analytics at a massive scale. It is an Apache Hadoop ecosystem component developed by Facebook to query the data stored in Hadoop Distributed File System (HDFS). Here, HDFS is the data storage layer of Hadoop that at the very high level. Hive is also termed as Data Warehousing framework of Hadoop and provides various analytical features, such as windowing and partitioning

Hive is built on top of Apache Hadoop. As a result, Hive is closely integrated with Hadoop, and is designed to work quickly on petabytes of data. What makes Hive unique is the ability to query large datasets, leveraging Apache Tez or MapReduce, with a SQL-like interface called HiveQL.

Traditional relational databases are designed for interactive queries on small to medium datasets and do not process huge datasets well. Hive instead uses batch processing so that it works quickly across a very large distributed database.

Hive transforms HiveQL queries into MapReduce or Tez jobs that run on Apache Hadoop’s distributed job scheduling framework, Yet Another Resource Negotiator (YARN). It queries data stored in a distributed storage solution, like the Hadoop Distributed File System (HDFS) or Amazon S3.



Hive stores the metadata of the databases and tables in a metastore, which is a database (for instance, MySQL) or file backed store that enables easy data abstraction and discovery. Hive includes HCatalog, which is a table and storage management layer that reads data from the Hive metastore to facilitate seamless integration between Hive, and MapReduce. By using the metastore, HCatalog allows MapReduce to use the same data structures as Hive, so that the metadata doesn’t have to be redefined for each engine. Custom applications or third party integrations can use WebHCat, which is a RESTful API for HCatalog to access and reuse Hive metadata.

Data in Apache Hive can be categorized into Table, Partition, and Bucket. The table in Hive is logically made up of the data being stored. It is of two types such as an internal or managed table and external table.

In this stage, we will create external Hive tables for the PostgreSQL tables imported by Sqoop. Then, we will perform Exploratory Data Analysis using HiveQL and visualize it using Apache superset.

Dataset Description
The dataset is about the departments and employees in a company as well as their salary categories. It consists of two .csv files.

Details
Note: We assume that your current working directory . is your repo root directory /home/teamx/project/ in the cluster.

Preparation
Before starting with Hive tables, make sure that you imported the PostgreSQL tables to HDFS (/user/teamx/project/warehouse warehouse folder, for instance) as serialized AVRO/PARQUET files and compressed using Snappy/Gzip/Bzip2 HDFS codec.

Using AVRO file format:
For AVRO file format, you also need to move the schemas .avsc from the local file system (it should be the folder where you executed sqoop command) to HDFS to a folder, for instance /user/teamx/project/warehouse/avsc, as follows (assuming that .avsc files are in the current working directory in the local file system):

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put output/*.avsc project/warehouse/avsc
Note: In the local filesystem, you should store the *.avsc and *.java output files in output/ folder of the project repository as follows:

mv *.avsc output/
mv *.java output/
Build Hive Tables
In this step, you need to write HiveQL statements for creating the Hive database (let’s call it teamx_projectdb) and reading the serialized and compressed table files. You can test the statements in an interactive mode using beeline tool but for project purposes, you need to write them in .hql files and then you execute the statements from the file using beeline tool or via hivejdbc library which should repeat all the steps. Here, we will store the HiveQL statements in a file db.hql.

HiveQL	Description
SHOW DATABASES;	Listing databases
USE teamx_projectdb;	Selecting the database teamx_projectdb
SHOW TABLES;	Listing tables in a database
DESCRIBE [FORMATTED|EXTENDED] depts;	Describing the format of a table depts
CREATE DATABASE teamx_projectdb;	Creating a database teamx_projectdb
DROP DATABASE db_name [CASCADE];	Dropping a database teamx_projectdb
I will give here some of the basic steps to create the Hive database teamx_projectdb but you need to extend it for your project purposes.

Drop the databases if it exists.
DROP DATABASE IF EXISTS teamx_projectdb;
If you got the following error

FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. InvalidOperationException(message:Database teamx_projectdb is not empty. One or more tables exist.)
Then add CASCADE as follows:

DROP DATABASE IF EXISTS teamx_projectdb CASCADE;
Create a database teamx_projectdb and access it.
CREATE DATABASE teamx_projectdb LOCATION "project/hive/warehouse";
USE teamx_projectdb;
This will create the database where the database tables are stored in HDFS in the folder project/hive/warehouse (This is a relative path to the home directory of the user in HDFS).

Create tables employees and departments
-- Create tables

-- emps table
-- AVRO file format
CREATE EXTERNAL TABLE employees STORED AS AVRO LOCATION 'project/warehouse/emps' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/emps.avsc');


-- dept table
-- AVRO file format
CREATE EXTERNAL TABLE departments STORED AS AVRO LOCATION 'project/warehouse/depts' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/depts.avsc');

We can check that tables are created by running some select queries.
-- For checking the content of tables
SELECT * FROM employees;
SELECT * FROM departments;
After you prepared the db.hql file, follow the next steps:

Read the password from password file.
password=$(head -n 1 secrets/.hive.pass)
Run the file db.hql via the command beeline -f as follows:
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n teamx -p $password -f sql/db.hql
The query results needs to be saved to hive_results.txt file. You can use the redirection operator (>) to store the output in a file as follows:
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n teamx -p $password -f sql/db.hql > output/hive_results.txt
Note: You should do the steps above for the project.

Note: If you want to omit the error stream of beeline command, redirect it into /dev/null as follows:

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n teamx -p $password -f sql/db.hql > output/hive_results.txt 2> /dev/null


Note: Do not forget, that the file .hql should not return errors when you run it for the second time, so you should clear/drop the objects before creating new database objects. This is true for all scripts in the pipeline.

We can also connect to Hive Server in Python as follows:

from hivejdbc import connect, DictCursor
import os


# Read password from secrets file
file = os.path.join("secrets", ".hive.pass")
with open(file, "r") as file:
        password=file.read().rstrip()
        
# Connect to HS2
conn = connect(
    host='hadoop-03.uni.innopolis.ru',
    port=10001,
    driver="/shared/hive-jdbc-3.1.3-standalone.jar",
    database='default',
    user='teamx',
    password=password
)


# Create a cursor
cur = conn.cursor()

# Execute one statement
cur.execute("SHOW DATABASES")


# Here we assume that this code is written in scripts/ or notebooks/ folder in the repository folder
repo_folder = os.path.join(".")
file_path = os.path.join(repo_folder, "sql", "db.hql")


# Read line by line
with open(file_path) as file:
    for line in file.readlines():
        
        # see note below
        line = line.replace(";", "")
        try:
            cur.execute(line)
            print(cur.fetchall())
        except:
            pass
Note: In hivejdbc package, you need to remove the semicolons (;) from each line before executing the HiveQL statement.

Hive Optimizations
Data in Apache Hive can be categorized into Table, Partition, and Bucket. The table in Hive is logically made up of the data being stored.

Partitioning – Apache Hive organizes tables into partitions for grouping same type of data together based on a column or partition key. Each table in the hive can have one or more partition keys to identify a particular partition. Using partition we can make it faster to run queries on slices of the data.

Bucketing – Hive Tables or partitions are subdivided into buckets based on the hash function of a column in the table to give extra structure to the data that may be used for more efficient queries.

Partitioning in Hive
Partitioning in Hive is used to increase query performance. Hive is very good tool to perform queries on large datasets, especially datasets that require a scan of an entire table. Generally, users are aware of their data domain, and in most of cases they want to search for a particular type of data. For such cases, a simple query takes large time to return the result because it requires the scan of the entire dataset. The concept of partitioning can be used to reduce the cost of querying the data. Partitions are like horizontal slices of data that allows the large sets of data as more manageable chunks. Table partitioning means dividing the table data into some parts based on the unique values of particular columns (for example, city and country) and segregating the input data records into different files or directories.

Partitioning in Hive is done using the PARTITIONED BY clause in the create table statement of HiveQL. Tables can have one or more partitions. A table can be partitioned on the basis of one or more columns. The columns on which partitioning is done cannot be included in the data of table. For example, you have the four fields id, name, age, and city, and you want to partition the data on the basis of the city field, then the city field will not be included in the columns of create table statement and will only be used in the PARTITIONED BY clause. You can still query the data in a normal way using where city=xyz. The result will be retrieved from the respective partition because data is stored in a different directory with the city name for each city.

Managed vs. External tables
There are two main types of tables in Hive—Managed tables and External tables. By default, Hive creates an Internal table also known as the Managed table, In the managed table, Hive owns the data/files on the table meaning any data you insert or load files to the table are managed by the Hive process when you drop the table the underlying data or files are also get deleted.

Using EXTERNAL option you can create an external table, Hive does not manage the external table, when you drop an external table, only table metadata from Metastore will be removed but the underlying files will not be removed and still they can be accessed from HDFS.

Create a Partitioned Table in Hive
For example, we have a table Employees containing the employee information of some company like empno, ename, job, deptno,…etc. Now, if we want to perform partitioning on the basis of deptno column. Then the information of all the employees belonging to a particular department will be stored together in a separate partition. Physically, a partition in Hive is nothing but just a sub-directory in the table directory. For example, we have data for three departments in our Employees table – Accounting (deptno=10), Reseaerch (deptno=20), Operations (deptno=40) and Sales (deptno=30). Thus we will have four partitions in total for each of the departments as we can see clearly in diagram below.



We can run the following statement to create the partitioned table.

CREATE EXTERNAL TABLE employees_part(empno int, ename varchar(50), job varchar(50), mgr int, hiredate date, sal decimal(10,2), comm decimal(10,2)) PARTITIONED BY (deptno int) STORED AS AVRO LOCATION 'project/hive/warehouse/employees_part' TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');


-- insert some data in nonstrict mode
insert into employees_part partition (deptno=20) values (7369,'SMITH','CLERK',7902,'93/6/13',800,0.00);
When you run the previous insert statement, you may get the following error.



This is due to the fact that strict mode is enabled but we can insert the data as follows:

-- insert some data in strict mode
insert into employees_part values (7369,'SMITH','CLERK',7902,'93/6/13',800,0.00,20);
Once nonstrict mode is enabled, we can create partitions for all unique values for any columns, say deptno of the employees_part table, as follows:

insert into employees_part partition (deptno) values (7369,'SMITH','CLERK',7902,'93/6/13',800,0.00,20);
We can insert the data from our unpartitioned table employees to the partitioned table employees_part.

-- Strict mode
INSERT INTO employees_part SELECT * FROM employees;


-- Nonstrict mode
INSERT INTO employees_part partition (deptno) SELECT * FROM employees;
The previous query would fail since the hiredate is stored as bigint and it is the milliseconds of the unix timestamp. You need to cast it into date using date time functions in Hive as follows:

INSERT INTO employees_part SELECT empno, ename, job, mgr, from_unixtime(FLOOR(CAST(hiredate AS BIGINT)/1000), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, deptno FROM employees;

-- Using CAST(hiredate AS BIGINT) is not needed here since we know that hiredate is stored as BIGINT but `hiredate/1000` returns double and here we need to cast to BIGINT as input to from_unixtime

INSERT INTO employees_part SELECT empno, ename, job, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, deptno FROM employees;
For each department we will have all the data regarding that department residing in a separate sub–directory under the table directory in HDFS as follows:

…/employees_part/deptno=10
…/employees_part/deptno=20
…/employees_part/deptno=30
…/employees_part/deptno=40
For instance, the queries regarding Sales department, we would only have to look through the data present in the Sales partition (deptno=30) as follows:

SELECT * FROM EMPLOYEES_PART WHERE deptno=30;
Therefore from above example, we can conclude that partitioning is very useful. It reduces the query latency by scanning only relevant partitioned data instead of the whole data set.

Static vs. Dynamic partitioning
Both table types support partitioning mechanism. Partitioning can be done in one of the following two ways:

Static partitioning
Dynamic partitioning
In static partitioning, you need to manually insert data in different partitions of a table. Let’s use a table partitioned on the departments of the company. For each department, you need to manually insert the data from the data source to a department partition in the partitioned table. So for 4 depts, you need to write the equivalent number of Hive queries to insert data in each partition.

Example on static partitioning
The table employees contains 3 unique departments deptno=10, deptno=20 and deptno=30 and 5 distinct jobs job=ANALYST, job=CLERK, job=MANAGER, job=PRESIDENT, and job=SALESMAN. In total, we will create at most 15 partitions.

-- Set the option
SET hive.exec.dynamic.partition=false;

-- Create a table with two partition columns (job, deptno)
CREATE EXTERNAL TABLE employees_part2(empno int, ename varchar(50), mgr int, hiredate date, sal decimal(10,2), comm decimal(10,2)) PARTITIONED BY (job varchar(50), deptno int) STORED AS AVRO LOCATION 'project/hive/warehouse/employees_part2' TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');
 
 
-- For each partition, we need to manually insert the data
-- The number of queries equals to number of partitions
 
-- insert data on a static partition (job='Analyst', deptno=10)
INSERT overwrite TABLE employees_part2
PARTITION (job='Analyst',deptno=10)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm FROM employees WHERE deptno=10 AND job="'ANALYST'";

-- insert data on a static partition (job='Analyst', deptno=20)
INSERT overwrite TABLE employees_part2
PARTITION (job='Analyst',deptno=20)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm FROM employees WHERE deptno=20 AND job="'ANALYST'";


-- insert data on a static partition (job='Analyst', deptno=30)
INSERT overwrite TABLE employees_part2
PARTITION (job='Analyst',deptno=30)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm FROM employees WHERE deptno=30 AND job="'ANALYST'";


-- insert data on a static partition (job='Clerk', deptno=10)
INSERT overwrite TABLE employees_part2
PARTITION (job='Clerk',deptno=10)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm FROM employees WHERE deptno=10 AND job="'CLERK'";


-- ....etc

-- You should write 15 queries like this
The partitions in HDFS will be stored as follows:


-- Displays the partitions of the table employees_part2
SHOW PARTITIONS employees_part2;
What if you have 1000 different departments and 10 different jobs or you have some other partitioning column like year and month? You have to create those manually and that is no FUN!! This is where dynamic partitioning helps us.

Prior to Hive 0.9.0, dynamic partitioning was disabled by default whereas it is enabled in Hive 0.9.0 and later by default. You can enable it by setting the following properties in beeline or in db.hql file:

SET hive.exec.dynamic.partition=true;
The configuration property hive.exec.dynamic.partition.mode allows to switch between strict and nonstrict modes of dynamic partition. In strict mode, the user must specify at least one static partition in case the user accidentally overwrites all partitions, in nonstrict mode all partitions are allowed to be dynamic. By default, the mode is strict and we can change it as follows:

SET hive.exec.dynamic.partition.mode=nonstrict;
Note: The static partition columns will be added as the last columns to the table.

Example on dynamic partitioning – strict mode
The table employees contains 3 unique departments deptno=10, deptno=20 and deptno=30 and 5 distinct jobs job=ANALYST, job=CLERK, job=MANAGER, job=PRESIDENT, and job=SALESMAN. In total, we will create at most 15 partitions.

-- Set the options
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=strict;

-- Create a table with two partition columns (job, deptno)
CREATE EXTERNAL TABLE employees_part2(empno int, ename varchar(50), mgr int, hiredate date, sal decimal(10,2), comm decimal(10,2)) 
PARTITIONED BY (job varchar(50), deptno int) 
STORED AS AVRO LOCATION 'project/hive/warehouse/employees_part2' 
TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

 
-- insert data using dynamic partitioning (job='Clerk', deptno) where the static partition job='Clerk' is a parent of the dynamic partition deptno (strict mode)
INSERT overwrite TABLE employees_part2
PARTITION (job='Clerk',deptno)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, deptno FROM employees WHERE job="'CLERK'";

-- insert data using dynamic partitioning (job='Analyst', deptno) where the static partition job='Analyst' is a parent of the dynamic partition deptno (strict mode)
INSERT overwrite TABLE employees_part2
PARTITION (job='Analyst',deptno)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, deptno FROM employees WHERE job="'ANALYST'";

-- insert data using dynamic partitioning (job='Manager', deptno) where the static partition job='Manager' is a parent of the dynamic partition deptno (strict mode)
INSERT overwrite TABLE employees_part2
PARTITION (job='Manager',deptno)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, deptno FROM employees WHERE job="'MANAGER'";

-- insert data using dynamic partitioning (job='President', deptno) where the static partition job='President' is a parent of the dynamic partition deptno (strict mode)
INSERT overwrite TABLE employees_part2
PARTITION (job='President',deptno)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, deptno FROM employees WHERE job="'PRESIDENT'";

-- insert data using dynamic partitioning (job='Salesman', deptno) where the static partition job='Salesman' is a parent of the dynamic partition deptno (strict mode)
INSERT overwrite TABLE employees_part2
PARTITION (job='Salesman',deptno)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, deptno FROM employees WHERE job="'SALESMAN'";


-- We needed 5 queries.
The partitions in HDFS will be stored as follows:


Note: If you try to execute the following query in strict mode.

INSERT overwrite TABLE employees_part2
PARTITION (job, deptno)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, job, deptno FROM employees;
You will get the following error:

Error: Error while compiling statement: FAILED: SemanticException [Error 10096]: Dynamic partition strict mode requires at least one static partition column. To turn this off set hive.exec.dynamic.partition.mode=nonstrict (state=42000,code=10096)
Note: If you try to execute the following query in strict mode.

INSERT overwrite TABLE employees_part2
PARTITION (job, deptno=10)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, job FROM employees WHERE deptno=10;
You will get the following error:

Error: Error while compiling statement: FAILED: SemanticException [Error 10094]: Line 2:11 Dynamic partition cannot be the parent of a static partition '10' (state=42000,code=10094)
Example on dynamic partitioning – nonstrict mode
The table employees contains 3 unique departments deptno=10, deptno=20 and deptno=30 and 5 distinct jobs job=ANALYST, job=CLERK, job=MANAGER, job=PRESIDENT, and job=SALESMAN. In total, we will create at most 15 partitions.

-- Set the option
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Create a table with two partition columns (job, deptno)
CREATE EXTERNAL TABLE employees_part2(empno int, ename varchar(50), mgr int, hiredate date, sal decimal(10,2), comm decimal(10,2)) 
PARTITIONED BY (job varchar(50), deptno int) 
STORED AS AVRO LOCATION 'project/hive/warehouse/employees_part2' 
TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');
 
-- insert data using dynamic partitioning (job, deptno) where both are dynamic partition columns (nonstrict mode)
INSERT overwrite TABLE employees_part2
PARTITION (job, deptno)
SELECT empno, ename, mgr, from_unixtime(CAST(hiredate/1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss.SSS') AS hiredate, sal, comm, job, deptno FROM employees;

-- We need only one query
The partitions in HDFS will be stored as follows:


Bucketing in Hive
In the scenario where we query on unique values of the partition column in partitioned table, partitioning is not a good fit. If we perform partitioning based on a column with a lot of unique values like ID, it would create a large number of small datasets in HDFS and partition entries in the metastore, thus increasing the load on NameNode and the metastore service. To optimize queries on such a dataset, we group the data into a particular number of buckets and the data is divided into the maximum number of buckets.

We can create buckets on empno column of employees_part as follows:

CREATE EXTERNAL TABLE employees_part_buck(
    empno int, 
    ename varchar(50), 
    job varchar(50), 
    mgr int, 
    hiredate date, 
    sal decimal(10,2), 
    comm decimal(10,2)
) 
    PARTITIONED BY (deptno int) 
    CLUSTERED BY (empno) into 7 buckets
    STORED AS AVRO LOCATION 'project/hive/warehouse/employees_part_buck' 
    TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');
Insert data from unpartitioned table.
INSERT INTO employees_part_buck
SELECT empno, ename, job, mgr,
    from_unixtime(CAST(hiredate/1000 AS BIGINT)) AS hiredate, 
    sal, comm, deptno 
FROM employees;
Query some data
SELECT * FROM employees_part_buck WHERE deptno=30 AND empno<7900;
Apache Tez
Hive supports two engines by default for running HiveQL queries. The default engine is Apache Tez SET hive.execution.engine=tez but if it is not working for some reason, then you can use the traditional MapReduce by changing the configuration SET hive.execution.engine=mr.

Tez is a new application framework built on Hadoop Yarn that can execute complex directed acyclic graphs of general data processing tasks. In many ways it can be thought of as a more flexible and powerful successor of the map-reduce framework.



It generalizes map and reduce tasks by exposing interfaces for generic data processing tasks, which consist of a triplet of interfaces: input, output and processor. These tasks are the vertices in the execution graph. Edges (i.e.: data connections between tasks) are first class citizens in Tez and together with the input/output interfaces greatly increase the flexibility of how data is transferred between tasks.

Tez also greatly extends the possible ways of which individual tasks can be linked together; In fact any arbitrary DAG can be executed directly in Tez.



In Tez, a map-reduce job is basically a simple DAG consisting of a single map and reduce vertices connected by a “bipartite” edge (i.e.: the edge connects every map task to every reduce task). Map input and reduce outputs are HDFS inputs and outputs respectively. The map output class locally sorts and partitions the data by a certain key, while the reduce input class merge-sorts its data on the same key.

Perform Exploratory Data Analysis (EDA)
EDA is a data analytics process to understand the data in depth and learn the different data characteristics, often with visual means. This allows you to get a better feel of your data and find useful patterns in it.

The concept of Online Analytical Processing (OLAP) has been widely discussed through the years and many papers have been written on the subject. OLTP is often used to handle large amounts of short and repetitive transactions in a constant flow, such as bank transactions or order entries. The database systems are designed to keep the data consistent and to maximize transaction throughput. OLAP databases are at the other hand used to store historical data over a long period of time, often collected from several data sources, and the size of a typical OLAP database is often orders of magnitude larger than that of an ordinary OLTP database. OLAP databases are not updated constantly, but they are loaded on a regular basis such as every night, every week-end or at the end of the month. This leads to few and large transactions, and query response time is more important than transaction throughput since querying is the main usage of an OLAP database. Hive supports Online Analytical Processing (OLAP), but not Online Transaction Processing (OLTP)



The core of the OLAP technology is the data cube, which is a multidimensional database model. The model consists of dimensions and numeric metrics which are referred to as measures. The measures are numerical data such as revenue, cost, sales and budget. Those are dependent upon the dimensions, which are used to group the data similar to the group by operator in relational databases. Typical dimensions are time, location and product, and they are often organized in hierarchies. A hierarchy is a structure that defines levels of granularity of a dimension and the relationship between those levels. A time dimension can for example have hours as the finest granularity, and higher up the hierarchy can contain days, months and years. When a cube is queried for a certain measure, ranges of one or several dimensions can be selected to filter the data. For more info on Multidimensional data analysis (read this book).

Here you can use HiveQL on Tez engine or prefer Spark SQL on Spark engine to perform the EDA. When you use Spark SQL, the queries are written in HiveQL and Spark does not support all features of HiveQL.

If we use HiveQL to analyze the data. We need to provide insights about the data. We will create charts for the insights using Apache Superset. Here I will give some examples. For each query qx we should do as follows:

Write the HiveQL query qx using SQL editor of Apache superset or any text editor and save the query in a file sql/q1.hql.
Create a managed or external Hive Table qx_results to store the results of the query. Add HiveQL statements to q1.hql.
Save the results of the query in the table qx_results. Add HiveQL statements to q1.hql.
Using beeline, Run the queries in q1.hql.
Also export the table qx__results to a file output/qx (for first query it is output/q1).
In Apache Superset, create a dataset for the table qx_results.
Create a chart for the dataset, add the name/description of the chart and save it.
Export the chart as qx.jpg and store it in output folder.
Note: This chart will be added to the dashboard in stage 4.

The implementation of the steps for q1 on my dataset.

Write the HiveQL query qx using SQL editor of Apache superset or any text editor and save the query in a file sql/q1.hql.
-- the query q1
SELECT dname,
SUM(sal) AS total_sal
FROM departments AS d
JOIN employees AS e ON d.deptno = e.deptno
GROUP BY dname
ORDER BY total_sal DESC
LIMIT 10;
Create a managed or external Hive Table qx_results to store the results of the query. Add HiveQL statements to q1.hql.
USE teamx_projectdb;

DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results(
Dname STRING,
Total_Salaries FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q1'; 
Save the results of the query in the table qx_results. Add HiveQL statements to q1.hql.

USE teamx_projectdb;

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT dname,
SUM(sal) AS total_sal
FROM departments AS d
JOIN employees AS e ON d.deptno = e.deptno
GROUP BY dname
ORDER BY total_sal DESC
LIMIT 10;

SELECT * FROM q1_results;

Using beeline, Run the queries in q1.hql as follows:
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n teamx -p $password -f sql/q1.hql
-- sql/q1.hql content

USE teamx_projectdb;

DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results(
Dname STRING,
Total_Salaries FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q1'; 

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT dname,
SUM(sal) AS total_sal
FROM departments AS d
JOIN employees AS e ON d.deptno = e.deptno
GROUP BY dname
ORDER BY total_sal DESC
LIMIT 10;

SELECT * FROM q1_results;

Also export the table qx_results to a file. We have two ways (You can use one of them):

Method 1: Redirecting the output of beeline from step 4 to a local file output/q1.csv. The file output/q1.csv contains only the output of the query and it is not a csv file.
Make sure that you have the following query in sql/q1.hql.
USE teamx_projectdb; 
SELECT * FROM q1_results;
Run the query as follows
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n teamx -p $password -f sql/q1.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q1.csv
The file output/q1.csv will be saved in the local file system.
Method 2: Exporting the table to a csv file output/q1.csv. The file output/q1.csv contains the csv representation of the table q1_results.
Make sure that you have the following query in sql/q1.hql.
USE teamx_projectdb;

INSERT OVERWRITE DIRECTORY 'project/output/q1' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q1_results;
Run the query as follows:
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n teamx -p $password -f sql/q1.hql
The folder project/output/q1 will contain the output data in HDFS. You can bring it into the local file system. Add a header to the csv file, concatenate all its partitions and redirect the output to the local file system.
# Add a header to the output file
echo "dname,total_salaries" > output/q1.csv 
# concatenate all file partitions and 
# append the output to the file output/q1.csv
hdfs dfs -cat project/output/q1/* >> output/q1.csv
In Apache Superset, create a dataset for the Hive table qx_results.


Create a chart for the dataset, add the name/description of the chart and save it.


Export the chart as qx.jpg and store it in output folder.


We can upload it to the output folder via Jupyter Lab as follows.



Important Notes:

In the project, you have to build at least one Hive table that supports partitioning and bucketing. The partitioning by one column is enough.
You should extract x different inisights from your data in the project where x is specified in your project description document.
Keep these charts for the Presentation stage (stage 4) where you will create the dashboard and add the charts.
In the project, you can write queries in HiveQL or Spark SQL.
In the project, you can store the query results (considered as not big data) in PostgreSQL or Hive.
If you store the data in PostgreSQL, use the same database created for you teamx_projectdb and also create dataset and chart using the database connection sql in Apache Superset.
If you want to use Spark SQL to run run queries on Hive data warehouse, then you need to:
Add Hive support for your app
from pyspark.sql import SparkSession
# The HDFS folder where your table data is stored 
warehouse = "project/hive/warehouse"


spark = SparkSession.builder \
    .master("yarn")\
    .appName("Spark SQL Hive")\
    .config("spark.sql.catalogImplementation","hive")\
    .config("hive.metastore.uris","thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", warehouse)\
    .enableHiveSupport() \
    .getOrCreate()
Use Spark SQL API to run HiveQL queries.
# List all databases in Hive metastore
spark.catalog.listDatabases()

# Switch to a database
spark.sql("USE teamx_projectdb;")

# List tables
spark.sql("show tables;").show()

# Run HiveQL queries
df = spark.sql("SELECT * FROM <TABLE-NAME>;")
df.show()

When you store the query results data in PostgreSQL, you have to use JDBC connection to access the PostgreSQL database. Spark SQL includes a data source that can read data from other databases using JDBC. You will need to include the JDBC driver for your particular database on the spark classpath. For example, we can store the query q1 results in PostgreSQL as follows:
Add the JDBC driver to app configs
from pyspark.sql import SparkSession
# The HDFS folder where your table data is stored 
warehouse = "project/hive/warehouse"


spark = SparkSession.builder \
    .master("yarn")\
    .appName("Spark SQL Hive")\
    .config("spark.sql.catalogImplementation", "hive")\
    .config("hive.metastore.uris","thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", warehouse)\
    .config("spark.driver.extraClassPath", "/shared/postgresql-42.6.1.jar")\
    .config("spark.jars", "/shared/postgresql-42.6.1.jar")\
    .enableHiveSupport() \
    .getOrCreate()
Prepare a DataFrame to store in the database
df2 = df.groupBy("job").sum("sal")
df2.show()
Write it to the database
df2.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb") \
    .option("dbtable", "<table-name>") \
    .option("user", "teamx") \
    .option("password", "<password>") \
    .mode("overwrite")\
    .save()
Check if you can read the table from PostgreSQL usin Spark SQL API
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://hadoop-04.uni.innopolis.ru/teamx_projectdb") \
    .option("dbtable", "<table-name>") \
    .option("user", "teamx") \
    .option("password", "<password>") \
    .load()
df.show()
Project Checklist - Stage 2
If you used AVRO format, put the *.avsc files from stage 1 to HDFS in a folder like project/warehouse/avsc.
Create a hive database like teamx_projectdb in project/hive/warehouse and access it.
Do not use same location in HDFS where you stored the tables imported via Sqoop.
Create external Hive tables for all the tables that are imported to HDFS in stage 1.
Check the datatypes of the columns of the new tables.
Maybe some columns needs conversion.
Create external, partitioned and bucketing Hive tables for one of the external Hive tables above.
The table should use partitioning and backeting.
If you have more than one table, then you may use partitioning in one table and bucketing in another table.
Check whether you can query data from the tables above.
Delete the unpartitioned Hive tables from your database and for the EDA use only partititioned and bucketing Hive tables.
Perform EDA. For each data insight x:
Write the query qx.
Run the query using beeline or Spark SQL.
Store results of the query qx in a Hive/PostgreSQL table qx_results.
Export the table qx_results to output/qx.csv.
Create the dataset and the chart in Apache Superset.
Export the chart as image qx.jpg and put it in output folder.
Write scripts to automate the tasks above except the tasks in Apache Superset of creating datasets and charts.
Run the script stage2.sh to test this stage.
Check the quality of scripts in this stage using pylint command.
Summarize your work in this stage and add it to the report.
References
Avro 1.8.1 Specification
Hive Language Manual
toy-dataset
Hive documentation
HiveQL Cheat Sheet
https://hashdork.com/apache-hive-tutorial/
static vs dynamic partitioning in Hive
Hive operators
Hive Tables in Spark SQL