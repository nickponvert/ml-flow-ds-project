from pyspark.sql import DataFrame, SparkSession
import json
from project.data.schema import load_pyspark_schema


def load_iris(cwd: str = "") -> DataFrame:
    """Load Iris data as spark sql dataframe

    :param cwd: Optional path to the project root
    :type cwd: str
    
    :returns: Spark SQL DataFrame
    :rtype: DataFrame

    :Example:

    +---------------+--------------+---------------+--------------+-----------+
    |sepal_length_cm|sepal_width_cm|petal_length_cm|petal_width_cm|      class|
    +---------------+--------------+---------------+--------------+-----------+
    |            5.1|           3.5|            1.4|           0.2|Iris-setosa|
    +---------------+--------------+---------------+--------------+-----------+
    """
    ds = "raw/iris"
    spark = SparkSession.builder.getOrCreate()
    return (
        spark.read.format("csv")
        .schema(load_pyspark_schema(ds, cwd))
        .load("{}data/data/{}".format(cwd, ds))
    )
