.PHONY: help
.PHONY: test

help: ## Shows this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

raw-data: ## Download the raw data
raw-data: data/data data/data/raw data/data/raw/iris

interim-data: ## Batch create interim data 
interim-data: raw-data data/data/interim data/schema/interim iris-features

processed-data: ## Batch create processed data
processed-data: interim-data data/data/processed data/schema/processed iris-scores

test: ## Run python unit tests
	python -m unittest;

FEATURE_RUNID?=f3a735a824264043a7978ee1aa0230d6
iris-features: data/data/interim/iris_features/3/_SUCCESS
data/data/interim/iris_features/3/_SUCCESS:
	docker exec -ti project_jupyter python zip_project.py;
	docker exec -ti project_jupyter \
		/usr/local/spark/bin/spark-submit --master local[*] \
		--py-files project.zip \
		project/data/features.py $(FEATURE_RUNID)

MODEL_RUNID?=9eb818da5f3d43848c9e105509b0a392
iris-scores: data/data/processed/iris_classification/3/_SUCCESS
data/data/processed/iris_classification/3/_SUCCESS:
	docker exec -ti project_jupyter python zip_project.py;
	docker exec -ti project_jupyter \
		/usr/local/spark/bin/spark-submit --master local[*] \
		--py-files project.zip \
		project/model/batch_score.py $(MODEL_RUNID) 3


jupyter: ## Start Jupyter+Spark Environment
	docker-compose up --no-recreate -d jupyter;
	@while [ `docker exec project_jupyter jupyter notebook list | wc -l` -eq "1" ]; do \
	echo "CREATING ..."; \
	sleep 3s; \
	done
	pipenv run pipenv_to_requirements -f -o requirements.txt;
	if [ -a requirements.txt ]; then docker exec project_jupyter pip install -r requirements.txt; fi;
	docker exec project_jupyter jupyter notebook list;


deploy-realtime-model: ## Start the services for realtime scoring
	$(eval TMP_FOLDER = $(shell mlflow artifacts download -r $(FEATURE_RUNID) -a feature_pipeline/mleap/model))
	$(info $(TMP_FOLDER))
	$(shell cd $(TMP_FOLDER); zip -qq -r feature_pipeline.zip .)
	docker run -d -p 5001:8080 -v $(TMP_FOLDER):/models --net project_default --name iris-features combustml/mleap-spring-boot:0.14.1-SNAPSHOT;
	sleep 10;
	curl --header "Content-Type: application/json" \
		--request POST \
		--data '{"modelName":"feature-pipeline","uri":"file:/models/feature_pipeline.zip","config":{"memoryTimeout":900000,"diskTimeout":900000},"force":true}' \
		http://localhost:5001/models;
	mlflow models build-docker -m "runs:/$(MODEL_RUNID)/iris_classification" -n "iris-classifier";	
	docker run -d -p 5002:8080 --net project_default --name iris-classifier  iris-classifier;
	
score-realtime-model: ## Score a single example request
	python project/model/realtime_score.py 5.1 3.5 1.4 0.2;

data/data:
	mkdir data/data;
data/data/raw:
	mkdir data/data/raw;
data/data/raw/iris:
	mkdir data/data/raw/iris;
	curl -o data/data/raw/iris/iris.data \
		https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data;

data/data/interim:
	mkdir data/data/interim;
data/data/processed:
	mkdir data/data/processed;
data/schema:
	mkdir data/schema;
data/schema/raw:
	mkdir data/schema/raw;
data/schema/interim:
	mkdir data/schema/interim;
data/schema/processed:
	mkdir data/schema/processed;