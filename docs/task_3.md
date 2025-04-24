Dataset Description
The dataset is about the departments and employees in a company. It consists of two .csv files.

Details
Preparation
Before starting with Spark ML, make sure that you built Hive tables and tested them via EDA in the previous stage.

Modeling in Spark ML
In this part of the project, we will build an ML model. Here I will explain two modes for performing predictive analysis in the cluster. The first one is used for production and the second one is used for development and debugging. We suggest to use both of them and perform the analysis in an interactive JupyterLab notebook then run the code via spark-submit.

Performing PDA should basically include (check the Project Checklist for the full list of steps):
- building the models.
- Tune the model hyper-parameters via grid search and cross validation.
- Compare models.
- Performing predictions on best models.

Note: For the project, you need to use Hive tables created in Hive via partitioning and/or bucketing. Check the project checklist.

1. Running Spark apps
1. Interactive analysis via jupyter notebook
Connect to Hive.
from pyspark.sql import SparkSession

# Add here your team number teamx
team = 

# location of your Hive database in HDFS
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName("{} - spark ML".format(team))\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()

#We can also add
# .config("spark.sql.catalogImplementation","hive")\ 
# But this is the default configuration
# You can switch to Spark Catalog by setting "in-memory" for "spark.sql.catalogImplementation"
Use Spark SQL to run HiveQL queries.
spark.sql("SHOW DATABASES").show()
spark.sql("USE teamx_projectdb").show()
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM <db_name>.<table_name>").show()
Note: The output for spark.sql function is a Spark DataFrame.

2. Non-interactive analysis via spark-submit
You can save the code above in a file scripts/model.py and run it on Spark using spark-submit tool as follows:

spark-submit --master yarn scripts/model.py
3. Interactive analysis via pyspark shell
Add Hive configs and start the shell
pyspark --master yarn --conf "spark.driver.extraJavaOptions=-Dhive.metastore.uris=thrift://hadoop-02.uni.innopolis.ru:9883"
Use Spark SQL to run HiveQL queries.
spark.sql("SHOW DATABASES").show()
spark.sql("USE teamx_projectdb").show()
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM <db_name>.<table_name>").show()
Note: You need to use jupyter notebook to analyze the data and then automate the tasks to run via spark-submit.

Note: Do not run spark applications via python3 but use spark-submit tool.

2. Read Hive tables
Connect to Hive.
from pyspark.sql import SparkSession

# Add here your team number teamx
team = 

# location of your Hive database in HDFS
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName("{} - spark ML".format(team))\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()

#We can also add
# .config("spark.sql.catalogImplementation","hive")\ 
# But this is the default configuration
# You can switch to Spark Catalog by setting "in-memory" for "spark.sql.catalogImplementation"
List all databases
print(spark.catalog.listDatabases())

# OR
# spark.sql("SHOW DATABASES;").show()
List all tables
print(spark.catalog.listTables("teamx_projectdb"))

# OR
# spark.sql("USE teamx_projectdb;")
# print(spark.sql("SHOW TABLES;"))
Read Hive table
emps = spark.read.format("avro").table('teamx_projectdb.employees_part')

# Creates a temporary view
# emps.createOrReplaceTempView('employees') 

depts = spark.read.format("avro").table('teamx_projectdb.departments')

# Creates a temporary view
# depts.createOrReplaceTempView('departments')
Now we can use depts and emps as input dataframes for our ML model.

Run some queries
emps.printSchema()
depts.printSchema()

spark.sql("SELECT * FROM employees WHERE deptno=10").show()

spark.sql("SELECT * FROM departments").show()

spark.sql("SELECT AVG(SAL) FROM employees;").show()

spark.sql("SELECT * from employees where comm is NULL;").show()
You can also run HiveQL queries here via Spark SQL too for EDA part of the project.

3. ML Modeling
Here I will predict the salaries of the employees.

1. Preprocessing the data
Note1: If you have time/datetime types in your dataset, then Sqoop probably will convert it to AVRO timestamp and you will get the time/datetime in unix_time format. You should not encode it as numerical feature and you need to convert it into time/datetime, decompose and encode its parts as cyclical features. You can use sin_cos_transformation as follows:

Decompose time/datetime field into six parts (year, month, days, hours, minutes, seconds). If you have only time, then it will be three parts.
You can encode the year part as a numerical feature since it is not cyclical.
For other parts (month, days, hours, minutes, seconds), build a custom pyspark.ml.Transformer for each part. Check this example to see how you can build a custom transformer.
This transformation encodes each input into two components, one component on sin wave and the other one on cos wave. For instance, the month part of the time/datetime field, after encoding, will have two columns as follows (monthsin, monthcos). It is given by
monthsin=pyspark.sql.functions.sin(2∗math.pi∗month12)
monthcos=pyspark.sql.functions.cos(2∗math.pi∗month12)
Note2: Scale the data if necessary. You can use scalers in pyspark.ml.feature to scale the data or any additional methods to increase the performance of the system.

Note3: If you have multiple tables, then you need to join them (if possible) to form one Dataframe for the ML task.

Note4: You cannot drop features from the input dataframe unless you use some feature selection methods like Variance Threshold Selector where you aim to improve your ML model performance. Such decisions need to be added to the report with appropriate explanation.
Note5: For geospatial features, you have to treat the latitude, longitude and altitude/height (ellipsoidal height) as one feature. You need to build a pyspark.ml.Transformer as described in Note1 to convert the geodetic coordinates (latitude, longitude, altitude) into ECEF coordinates (x, y, z). Check this link for more info. Note that if you do not have altitude then you can place altitude as 0 or average ellipsoidal height for earth surface. Afterwards, you treat (x, y, z) as numerical features. If you are interested in using different coodinate systems, feel free to use it but make sure to not distort the characteristics of the geospatial features by encoding. The encoding procedure should preserve the characteristics of the features.

A. Selecting the features
# We will use the following features
# Excluded 'comm' because it has a lot of nulls
# Excuded hiredate because it is given as practice to implement the cos_sin_transformation for the student
features = ['empno', 'ename', 'job', 'mgr', 'deptno']

# The output/target of our model
label = 'sal'

import pyspark.sql.functions as F

# Remove the quotes before and after each string in job and ename columns.
emps = emps.withColumn("job", F.translate("job","'",""))
emps.show()
emps = emps.withColumn("ename", F.translate("ename","'",""))
emps.show()


# I am thinking of generating a new column out of ename and job.
# The column will have ename and job concatenated with '_' 
# Then we use word2Vec to encode it
emps = emps.select(features + [label]).na.drop()
emps = emps.withColumn("ename_job", F.concat(F.col('ename'), F.lit("_"), F.col('job')))
emps = emps.withColumnRenamed("sal","label")

emps.show()
B. Building the Pipeline

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Word2Vec, Tokenizer, RegexTokenizer
from pyspark.sql.functions import col

categoricalCols = ['deptno']
textCols = ['ename_job']
others = ['empno', 'mgr']


# Since the tokenizer only return tokens separated by white spaces, I used RegexTokenizer to tokenize by '_'
# Then created word2Vec model

# tokenizer = Tokenizer(inputCol="ename", outputCol="ename_tokens")
# emps_tok = tokenizer.transform(emps)
tokenizer = RegexTokenizer(inputCol=textCols[0], outputCol="ename_job_tokens", pattern="_")
# emps_tok = tokenizer.transform(emps)
# emps_tok.show()

word2Vec = Word2Vec(vectorSize=5, seed=42, minCount=1, inputCol="ename_job_tokens", outputCol="ename_enc")
# word2VecModel = word2Vec.fit(emps_tok)
# print(word2VecModel)

# emps_tok = word2VecModel.transform(emps_tok)
# emps_tok.show()

# Adding the encoded ename_job to the list of other columns
# others += [ename_enc]


# Create String indexer to assign index for the string fields where each unique string will get a unique index
# String Indexer is required as an input for One-Hot Encoder 
# We set the case as `skip` for any string out of the input strings
indexers = [ StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)).setHandleInvalid("skip") for c in categoricalCols ]

# Encode the strings using One Hot encoding
# default setting: dropLast=True ==> For example with 5 categories, an input value of 2.0 would map to an output vector of [0.0, 0.0, 1.0, 0.0]. The last category is not included by default (configurable via dropLast), because it makes the vector entries sum up to one, and hence linearly dependent. So an input value of 4.0 maps to [0.0, 0.0, 0.0, 0.0].
encoders = [ OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol="{0}_encoded".format(indexer.getOutputCol())) for indexer in indexers ]

# This will concatenate the input cols into a single column.
assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders] + others, outputCol= "features")

# You can create a pipeline to use only a single fit and transform on the data.
pipeline = Pipeline(stages=[tokenizer, word2Vec] + indexers + encoders + [assembler])


# Fit the pipeline ==> This will call the fit functions for all transformers if exist
model=pipeline.fit(emps)
# Fit the pipeline ==> This will call the transform functions for all transformers
data = model.transform(emps)

data.show()

# We delete all features and keep only the features and label columns
data = data.select(["features", "label"])


from pyspark.ml.feature import VectorIndexer

# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4
# distinct values are treated as continuous.
featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)
transformed = featureIndexer.transform(data)

# Display the output Spark DataFrame
transformed.show()
Split the dataset
#  split the data into 60% training and 40% test (it is not stratified)
(train_data, test_data) = transformed.randomSplit([0.6, 0.4], seed = 10)


# A function to run commands
import os
def run(command):
    return os.popen(command).read()

train_data.select("features", "label")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/train")

# Run it from root directory of the repository
run("hdfs dfs -cat project/data/train/*.json > data/train.json")

test_data.select("features", "label")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/test")

# Run it from root directory of the repository
run("hdfs dfs -cat project/data/test/*.json > data/test.json")
2. Modeling
Here I will show one model type to predict the salaries of the employees via linear regression. In the notebook template in folder /shared/ml.ipynb of the cluster, you will see that two models are built and that is required for the project.

2.1. Model training
from pyspark.ml.regression import LinearRegression
# Create Linear Regression Model
lr = LinearRegression()

# Fit the data to the lr model
model_lr = lr.fit(train_data)
2.2. Prediction
# Transform the data (Prediction)
predictions = model_lr.transform(testData)

# Display the predictions
predictions.show()
2.3. Evaluation
from pyspark.ml.evaluation import RegressionEvaluator 

# Evaluate the performance of the model
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
2.4. Hyperparameter optimization
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator 

import numpy as np


grid = ParamGridBuilder()
grid = grid.addGrid(
                    model_lr.aggregationDepth, [2, 3, 4])\
                    .addGrid(model_lr.regParam, np.logspace(1e-3,1e-1)
                    )\
                    .build()

cv = CrossValidator(estimator = lr, 
                    estimatorParamMaps = grid, 
                    evaluator = evaluator1,
                    parallelism = 5,
                    numFolds=3)

cvModel = cv.fit(train_data)
bestModel = cvModel.bestModel
bestModel
2.5. Select best model
from pprint import pprint
model1 = bestModel
pprint(model1.extractParamMap())
2.6. Save the model to HDFS
model1.write().overwrite().save("project/models/model1")

# Run it from root directory of the repository
run("hdfs dfs -get project/models/model1 models/model1")
2.7. Prediction
predictions = model1.transform(test_data)
predictions.show()

predictions.select("label", "prediction")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .save("project/output/model1_predictions.csv")

# Run it from root directory of the repository
run("hdfs dfs -cat project/output/model1_predictions.csv/*.csv > output/model1_predictions.csv")
2.8. Evaluation
from pyspark.ml.evaluation import RegressionEvaluator 

# Evaluate the performance of the model
evaluator1 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse1 = evaluator1.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse1)
Here I put the code of only the first model but in the template you can find the second model.

Compare best models
# Create data frame to report performance of the models
models = [[str(model1),rmse1, r21], [str(model2),rmse2, r22]]

#temp = list(map(list, models.items()))
df = spark.createDataFrame(models, ["model", "RMSE", "R2"])
df.show(truncate=False)
#temp


# Save it to HDFS
df.coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .save("project/output/evaluation.csv")

# Run it from root directory of the repository
run("hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv")
The dataframe would look like as follows:


Upcoming lab
Build a Dashboard for presentation stage.
Presenting the project results and data insights in the dashboard.
Project checklist
Check the notebook /shared/ml.ipynb in the cluster for a template for this stage.

Read the hive tables as dataframes.
Build and fit a feature extraction pipeline.
Split the input dataset into train and test datasets. Save them in HDFS as json files like project/data/train, project/data/test and put them later in data/train.json, data/test.json files of the repository.
Select two types of ML models based on the ML task specified in project.info sheet.
The model type should be from pyspark.ml or pyspark.mllib
Or any 3rd party library where you need to verify its usage with the TA and the library should train the model distributively on the cluster.
First model type.
Build and train the model.
Predict for the test data.
Evaluate the model.
Specify at least 2 hyperparameters for it and the settings of grid search and cross validation.
The hyperparameters should be related to the model type and not a training parameter like max iterations.
Optimize its hyperparameters using cross validation and grid search on the training data only.
Use one of the optimization metrics.
You cannot use the test data here.
Select the best model (model1) from grid search.
Save the model1 to HDFS in location like project/models/model1 and later put it in models/model1 folder in the repository.
Predict for the test data using the model1.
Save the prediction results in HDFS in a CSV file like project/output/model1_predictions and later save it in output/model1_predictions.csv folder in the repository…
Keep only label and prediction columns.
Save it as one partition.
Evaluate the best model (model1) on the test data.
Second model type.
Build and train the model.
Predict for the test data.
Evaluate the model.
Specify at least 2 hyperparameters for it and the settings of grid search and cross validation.
The hyperparameters should be related to the model type and not a training parameter like max iterations.
Optimize its hyperparameters using cross validation and grid search on the training data only.
Use one of the optimization metrics.
You cannot use the test data here.
Select the best model (model2) from grid search.
Save the model2 to HDFS in location like project/models/model2 and later put it in models/model2 folder in the repository.
Predict for the test data using the model2.
Save the prediction results in HDFS in a CSV file like project/output/model2_predictions and later save it in output/model2_predictions.csv folder in the repository.
Keep only label and prediction columns.
Save it as one partition.
Evaluate the best model (model2) on the test data.
Compare the models (model1, model2) on the test data.
The comparison is based on the evaluation results.
Create a dataframe as follows:

This is just an example
Store the dataframe in HDFS in location project/output/evaluation and put it in CSV file output/evaluation.csv of project repository.
Write scripts to automate the tasks above.
Run the script stage3.sh to test this stage.
Check the quality of scripts in this stage using pylint command.
Summarize your work in this stage and add it to the report.