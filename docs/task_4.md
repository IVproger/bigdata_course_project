Stage IV - Presentation & Delivery
Course: Big Data - IU S24
Author: Firas Jolha

Dataset
Some emps and depts
Agenda
Stage IV - Presentation & Delivery
Dataset
Agenda
Prerequisites
Objectives
Description
Create a Dashboard
Presenting data description
Presenting data insights
Presenting ML modeling results
Dashboard storytelling
Project checklist
References
Prerequisites
Stage I is done
Stage II is done
Stage III is done
Objectives
Build a web dashboard in Apache Superset and publish it
Description
In this stage, you need to build a dashboard to present your analysis results. Dashboards are single screens in which various critical pieces of information are placed in the form of panels.

For the project purposes, you have to present at least the results of EDA and PDA, in addition to data characteristics but try to build a cool dashboard and present more about your findings in the project. Your objective here should be to impress the business stakeholders and provide them with insights which can help them take decisions.

Note: If you want to create a chart for data stored in tabular form (*.csv, *.json,…etc), you need to store it as a table in your hive database.

Create a Dashboard
You can easily create a dashboard in Apache Superset from Dashboards tab as follows:


You can add your project title as title for the dashboard. In this window, you can see your charts and also Layout elements for organizing your charts/panels in the dashboard.


Apache Superset provides the following layout elements:



Tabs divide the dashboard into separate screens where you can let the audience focus on specific results per screen.
Row is a horizontal layout which allows to store elements of the dashboard in rows.
Column is a vertical layout which allows to store elements of the dashboard in columns.
Header is a text element used to specify headers/titles for different sections of the dashboard.
Text is a text element used to write markdown text.
Divider is used to separate the dashboard sections.
Use the layout elements to organize your charts and to build cool dashboards.

You can create CSS templates and edit the CSS of the dashboard.




Presenting data description
In this part of the dashboard, you need to present the characteristics of the dataset and data features that you got from public sources. This section should present the description of the initial dataset in addition to some samples from the data.

You can use SQL Editor of Apache Superset to query the data then save it as a dataset.


You can query the datatypes of a table in your psql database as follows:

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'tabe_name';
For table emps, it would be:

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'emps';
Presenting data insights
Here you add the charts you built in stage II. You also need to add a conclusion for each data insight.

In the figure below, you see the charts and conclusion for one data insight.


Presenting ML modeling results
In this section, you present the features after feature extraction, the performance of models, prediction results for some data samples.

For *.csv files that you stored in HDFS in stage III, you create external Hive tables and create datasets and charts for them.

Dashboard storytelling
A dashboard helps you to monitor events or activities at a glance by providing key insights and analysis about your data on one or more pages or screens. You can explore the data that is shown in a visualization by using the interactive title, drilling up or down columns, and viewing the details of a data point.

You can change the visualization type or change the columns that are used in the visualization. You can use filters to focus on one area of your data or to see the impact of one column, and you can use calculations to answer questions that cannot be answered by the source columns.

A story is a type of view that contains a set of scenes that are displayed in sequence over time.

Stories are similar to dashboards because they also use visualizations to share your insights. Stories differ from dashboards because they provide an over-time narrative and can convey a conclusion or recommendation.

Project checklist
Create a dashboard and add your project title.
Organize the layout of your dashboard.
You do not need to follow my layout and feel free to customize your layout.
Create charts for tables stored in your Psql database. Add the charts to data description part.
Check that data description part presents information about:
Number of records per table
Datatypes of the table columns
Some data samples from tables
Any data cleaning you did before you build the SQL tables.
Add charts you built in stage II to data insights section of the dashboard.
Create external Hive tables for results of stage III.
Create charts for the hive tables of stage III.
The characteristics of feature extraction.
The results of hyper-parameter optimization.
The prediction results for each best model.
The evaluation results for each best model.
The comparison results of best models.
Add the charts to the ML modeling part of the dashboard.
Pubish the dashboard. Notice that this will make it visible to everyone.
Write scripts to automate the tasks above except the tasks in Apache Superset.
Run the script stage4.sh to test this stage.
Check the quality of scripts in this stage using pylint command.
Summarize your work in this stage and add it to the report.