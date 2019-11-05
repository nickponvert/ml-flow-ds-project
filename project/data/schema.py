import json
import pyspark
import pyspark.sql.types as T


def load_pyspark_schema(ds: str, cwd: str = "") -> T.StructType:
    """Create Spark StructType from JSON schema

    :param ds: The name of the dataset, e.g. raw/iris
    :type ds: str
    :param cwd: Optional path to the project root
    :type cwd: str
    
    :returns: A Spark SQL StructType
    :rtype: StructType
    """
    fp = "{}data/schema/{}/spark_sql_schema.json".format(cwd, ds)
    with open(fp, "r") as file:
        schema_str = file.read()
    return T.StructType.fromJson(json.loads(schema_str))
