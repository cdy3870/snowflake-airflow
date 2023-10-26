import logging
import sys
import warnings
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
import preprocess_data as preproc
# from mlflow_provider.hooks.client import MLflowClientHook

import snowflake_ingest
import preprocess_data


logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

mlflow.set_tracking_uri('http://host.docker.internal:5000')

def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2

def execute_experiment():
    warnings.filterwarnings("ignore")
    np.random.seed(40)
    # mlflow.set_tracking_uri("http://host.docker.internal:5000")


    # X, y = preproc.feature_extract(data)
    data = snowflake_ingest.query_processed()
    print(data)

    y = data["HEIGHT"]
    X = data.drop("HEIGHT", axis=1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4)
    y_train, y_test = preprocess_data.standardize_data(X_train, X_test)
    alpha = 0.5
    l1_ratio = 0.5

    with mlflow.start_run():
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(X_train, y_train)

        y_pred = lr.predict(X_test)
        (rmse, mae, r2) = eval_metrics(y_test, y_pred)

        print(f"Elasticnet model (alpha={alpha:f}, l1_ratio={l1_ratio:f}):")
        print(f"  RMSE: {rmse}")
        print(f"  MAE: {mae}")
        print(f"  R2: {r2}")

        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

    #     predictions = lr.predict(train_x)
    #     signature = infer_signature(train_x, predictions)

        # tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme

        # # Model registry does not work with file store
        # if tracking_url_type_store != "file":
        #     # Register the model
        #     # There are other ways to use the Model Registry, which depends on the use case,
        #     # please refer to the doc for more information:
        #     # https://mlflow.org/docs/latest/model-registry.html#api-workflow
        #     mlflow.sklearn.log_model(
        #         lr, "model", registered_model_name="ElasticnetWineModel"
        #     )
        # else:
        #     mlflow.sklearn.log_model(lr, "model")

if __name__ == "__main__":
    execute_experiment()