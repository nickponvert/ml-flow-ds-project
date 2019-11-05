import requests
import argparse
import os
from typing import List


FEATURE_MODEL_URI = os.environ.get("FEATURE_MODEL_URI")
CLASSIFIER_MODEL_URI = os.environ.get("CLASSIFIER_MODEL_URI")


def score(input_data: List[List[float]]) -> List[str]:
    """Wrapper for calling model scoring APIs

    :param input_data: Iris raw data
    :type input_data: List[List[float]]

    :returns: List of Predictions
    :rtype: List[str]
    """

    # check input_data is a nested list of rows
    assert any(isinstance(i, list) for i in input_data)

    feature_request = requests.post(
        #'http://localhost:5001/models/feature-pipeline/transform',
        FEATURE_MODEL_URI,
        json={
            "schema": {
                "fields": [
                    {"name": "sepal_length_cm", "type": "double"},
                    {"name": "sepal_width_cm", "type": "double"},
                    {"name": "petal_length_cm", "type": "double"},
                    {"name": "petal_width_cm", "type": "double"},
                ]
            },
            "rows": input_data,
        },
        headers={"Content-Type": "application/json"},
    )

    if feature_request.status_code != 200:
        raise Exception("Feature Error {}".format(feature_request.status_code))

    feature_response = list(
        map(lambda row: row[-1]["values"], feature_request.json()["rows"])
    )

    scoring_request = requests.post(
        CLASSIFIER_MODEL_URI,
        json=feature_response,
        headers={"Content-Type": "application/json; format=pandas-records"},
    )

    if scoring_request.status_code != 200:
        raise Exception("Scoring Error {}".format(scoring_request.status_code))

    return scoring_request.json()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Classify Iris Input")
    parser.add_argument(
        "features",
        metavar="F",
        type=float,
        nargs=4,
        help="The 4 iris raw features in cm e.g. 5.1 3.5 1.4 0.2",
    )
    args = parser.parse_args()

    input_data = args.features

    predictions = score([input_data])
    print(input_data, " --> ", predictions)
