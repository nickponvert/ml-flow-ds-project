import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from project.data import features
import mlflow


vec2array_udf = F.udf(lambda x: x.toArray().tolist(), T.ArrayType(T.DoubleType()))

if __name__ == "__main__":

    import os, sys
    import json
    import argparse
    import pathlib
    from project.utility.spark import get_or_create_spark_session

    parser = argparse.ArgumentParser(description="Batch feature scoring")
    parser.add_argument(
        "run_id",
        metavar="R",
        type=str,
        help="the run_id of the trained classification model to use",
    )
    parser.add_argument(
        "degree",
        metavar="D",
        type=int,
        default=3,
        help="the degrees of the polynomial feature expansion",
    )
    args = parser.parse_args()
    run_id = args.run_id
    degree = args.degree

    experiment = "iris_classification"
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    spark = get_or_create_spark_session()
    model_udf = mlflow.pyfunc.spark_udf(
        spark=spark,
        model_uri="runs:/{}/iris_classification".format(run_id),
        result_type="string",
    )

    feature_df = features.load_iris_features(degree=degree)
    score_df = (
        feature_df.withColumn("featureArray", vec2array_udf(F.col("features")))
        .withColumn(
            "prediction",
            model_udf(*map(lambda i: F.col("featureArray").getItem(i), range(34))),
        )
        .select(
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "prediction",
        )
    )

    (
        score_df.write.format("json").save(
            "data/data/processed/{}/{}".format(experiment, degree)
        )
    )
    score_schema = score_df.schema.json()
    schema_path = "data/schema/processed/{}/{}".format(experiment, degree)
    pathlib.Path(schema_path).mkdir(parents=True, exist_ok=True)
    with open("{}/spark_sql_schema.json".format(schema_path), "w") as f:
        f.write(score_schema)
