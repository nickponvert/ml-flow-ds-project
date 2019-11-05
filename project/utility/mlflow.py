import os
import posixpath
from minio import Minio
import mlflow
from mlflow.utils.file_utils import relative_path_to_artifact_path
from mlflow.data import parse_s3_uri
from urllib.parse import urlparse
import shutil


def log_artifacts_minio(
    run: mlflow.entities.Run,
    local_dir: str,
    artifact_path: str = None,
    delete_local: bool = True,
) -> None:
    """Upload local artefacts via Minio client
    This is needed as boto3 and Minio have problems with empty files. See

    - https://github.com/minio/minio/issues/5150
    - https://github.com/boto/botocore/pull/1328  

    :param run: an active Mlflow Run
    :type run: mlflow.entities.Run 
    :param local_dir: the path to the local directory with artifacts to log to Mlflow
    :type local_dir: str
    :param artifact_path: relative path of logged artifacts in Mlflow Run assets
    :type artifact_path: str
    :param delete_local: whether to delete the local assets after logging them to Mlflow
    :type delete_local: bool
    """
    (bucket, dest_path) = parse_s3_uri(run.info.artifact_uri)
    if artifact_path:
        dest_path = posixpath.join(dest_path, artifact_path)
    minio_client = Minio(
        urlparse(os.environ["MLFLOW_S3_ENDPOINT_URL"]).netloc,
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        secure=False,
    )
    local_dir = os.path.abspath(local_dir)
    for (root, _, filenames) in os.walk(local_dir):
        upload_path = dest_path
        if root != local_dir:
            rel_path = os.path.relpath(root, local_dir)
            rel_path = relative_path_to_artifact_path(rel_path)
            upload_path = posixpath.join(dest_path, rel_path)
        for f in filenames:
            minio_client.fput_object(
                bucket, posixpath.join(upload_path, f), os.path.join(root, f)
            )
    if delete_local:
        shutil.rmtree(local_dir)
