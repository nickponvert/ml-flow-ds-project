from pyspark.sql import DataFrame, SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    PolynomialExpansion,
    StringIndexer,
)
import pyspark.sql.functions as F
import pandas as pd
import tempfile
import mlflow
import mlflow.spark
from project.utility.mlflow import log_artifacts_minio
from project.data import raw, schema


def load_iris_features(degree: int = 3, cwd: str = "") -> DataFrame:
    """Load Iris features as spark sql dataframe
    :param cwd: Optional path to the project root
    :type cwd: str
    :param degree: degree of polynomial expansion
    :type degree: int
    
    :returns: Spark SQL DataFrame
    :rtype: DataFrame
    """
    ds = "interim/iris_features/{}".format(degree)
    spark = SparkSession.builder.getOrCreate()
    return (
        spark.read.format("json")
        .schema(schema.load_pyspark_schema(ds, cwd))
        .load("{}data/data/{}".format(cwd, ds))
    )


def train_new_feature_pipeline(df: DataFrame, degree: int = 3) -> PipelineModel:
    """Create a new feature pipeline and fit to training data

    :param df: raw Iris spark sql data frame
    :type df: DataFrame
    :param degree: degree of polynomial feature expansion
    :type degree: int

    :returns: fitted feature pipeline
    :rtype: PipelineModel    
    """
    assembler = VectorAssembler(
        inputCols=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
        ],
        outputCol="features",
    )
    scaler = StandardScaler(
        inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True
    )
    polyExpansion = PolynomialExpansion(
        degree=degree, inputCol="scaledFeatures", outputCol="polyFeatures"
    )
    pipeline = Pipeline(stages=[assembler, scaler, polyExpansion])
    pipeline_model = pipeline.fit(df)
    return pipeline_model


def mlflow_log_feature_training(
    tracking_uri: str, experiment: str, degree: int = 3
) -> str:
    """
    :param degree: degree of polynomial feature expansion
    :type degree: int

    :returns: Mlflow run_id
    :rtype: str
    """
    iris_df = raw.load_iris()

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    iris_df = raw.load_iris()
    feature_pipeline = train_new_feature_pipeline(iris_df, degree)

    tmp_path = tempfile.mkdtemp()
    with mlflow.start_run() as run:
        mlflow.log_param("degree", degree)
        mlflow.spark.save_model(
            feature_pipeline,
            tmp_path,
            sample_input=iris_df.select(
                "sepal_length_cm", "sepal_width_cm", "petal_length_cm", "petal_width_cm"
            ),
        )
        log_artifacts_minio(
            run=run,
            local_dir=tmp_path,
            artifact_path="feature_pipeline",
            delete_local=True,
        )
    return run.info.run_id


if __name__ == "__main__":

    import os, sys
    import json
    import argparse
    import pathlib
    from mlflow.tracking import MlflowClient
    from project.utility.spark import get_or_create_spark_session

    parser = argparse.ArgumentParser(description="Batch feature engineering")
    parser.add_argument(
        "run_id",
        metavar="R",
        type=str,
        help="the run_id of the trained feature pipeline to use",
    )
    args = parser.parse_args()
    run_id = args.run_id

    experiment = "iris_features"
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)
    mlflow_client = MlflowClient()
    run = mlflow_client.get_run(run_id)
    degree = run.data.params["degree"]

    _ = get_or_create_spark_session()

    iris_df = raw.load_iris()
    feature_pipeline = mlflow.spark.load_model(
        "runs:/{}/feature_pipeline".format(run_id)
    )
    feature_df = feature_pipeline.transform(iris_df).select(
        F.col("class"),
        F.col("sepal_length_cm"),
        F.col("sepal_width_cm"),
        F.col("petal_length_cm"),
        F.col("petal_width_cm"),
        F.col("polyFeatures").alias("features"),
    )

    (
        feature_df.write.format("json").save(
            "data/data/interim/{}/{}".format(experiment, degree)
        )
    )

    feature_schema = feature_df.schema.json()
    schema_path = "data/schema/interim/{}/{}".format(experiment, degree)
    pathlib.Path(schema_path).mkdir(parents=True, exist_ok=True)
    with open("{}/spark_sql_schema.json".format(schema_path), "w") as f:
        f.write(feature_schema)
