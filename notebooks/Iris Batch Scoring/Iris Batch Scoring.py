#!/usr/bin/env python
# coding: utf-8

# %load snippet/default_notebook_setup.py
## Not all libraries support reloads. This might break things
#%reload_ext autoreload
#%autoreload 2
get_ipython().run_line_magic('load_ext', 'dotenv')

import sys
sys.path.append('/home/jovyan')

get_ipython().run_line_magic('dotenv', '')


# %load snippet/default_spark.py
import pyspark
from pyspark.sql import SparkSession, Window, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession.builder
    .config('spark.jars.packages', 'ml.combust.mleap:mleap-spark-base_2.11:0.14.0,ml.combust.mleap:mleap-spark_2.11:0.14.0')
    .config('spark.sql.execution.arrow.enabled', 'true')
    .getOrCreate()
)


import os 
import mlflow
import mlflow.spark
import mlflow.pyfunc

from project.data import raw


iris_df = raw.load_iris('../')


# # Load serialised models
# 
# We load 
# 
# 1. the fitted spark feature pipeline. This model had been logged in the spark flavour and we can use it as is.
# 1. the sklearn classification model. This model had been logged  in the python flavour. To use it for batch scoring with Spark we create an UDF to upload our model to Spark for scoring.

feature_pipeline_run_id = 'f3a735a824264043a7978ee1aa0230d6'
feature_pipeline = mlflow.spark.load_model('runs:/{}/feature_pipeline'.format(feature_pipeline_run_id))


model_run_id = '9eb818da5f3d43848c9e105509b0a392'
model_udf = mlflow.pyfunc.spark_udf(
    spark=spark,
    model_uri='runs:/{}/iris_classification'.format(model_run_id), 
    result_type='string'
)


# # Batch Scoring
# 
# The Spark feature pipeline returns a spark dense vector. We have to turn the vectors into arrays first to be able to access them as needed to pass the features to our classifier. It's important to check version compatibiity of PyArrow and PySpark, e.g. PySpark 2.4 only works with PyArrow <= 0.14.1
# 
# With Mlflow 1.3 the created model udf expects features as individual arguments rather than as an array.

vec2array_udf = F.udf(lambda x: x.toArray().tolist(), T.ArrayType(T.DoubleType()))


(
    feature_pipeline.transform(iris_df)
    .withColumn('polyFeatureArray', vec2array_udf(F.col('polyFeatures')))
    .withColumn('prediction', model_udf(
        *map(lambda i: F.col('polyFeatureArray').getItem(i), range(34)))
    )
    .select('sepal_length_cm', 'sepal_width_cm', 'petal_length_cm', 'petal_width_cm', 'class', 'prediction')
).show()




