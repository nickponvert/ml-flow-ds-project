# ml-flow-ds-project

Forked from: https://gitlab.com/jan-teichmann/ml-flow-ds-project

An example data science project using Mlflow.

Detailed blog post on: https://towardsdatascience.com/complete-data-science-project-template-with-mlflow-for-non-dummies-d082165559eb

Enable the virtual python environment with
```
pipenv shell
```
and setup a local environment with
```
pipenv install --dev
```
download the raw data
```
make raw-data
```
and start the project services with
```
make jupyter
```

All further project targets are automated in the Makefile and via Jupyter notebooks.
