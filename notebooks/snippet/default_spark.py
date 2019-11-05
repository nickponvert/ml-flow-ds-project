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