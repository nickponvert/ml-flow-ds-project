import pyspark
from pyspark.sql import SparkSession


def get_or_create_spark_session() -> SparkSession:
    """Get or Create a Spark SQL session
    This adds support for MLeap and AWS S3.

    :returns: Spark SQL session
    :rtype: SparkSession
    """
    return SparkSession.builder.config(
        "spark.jars.packages",
        "ml.combust.mleap:mleap-spark-base_2.11:0.14.0,ml.combust.mleap:mleap-spark_2.11:0.14.0,org.apache.hadoop:hadoop-aws:2.7.3",
    ).getOrCreate()
