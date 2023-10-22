"""
### Show three ways to use MLFlow with Airflow

This DAG shows how you can use the MLflowClientHook to create an experiment in MLFlow,
directly log metrics and parameters to MLFlow in a TaskFlow task via the mlflow Python package, and
create a new model using the CreateRegisteredModelOperator of the MLflow Airflow provider package.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from mlflow_provider.hooks.client import MLflowClientHook
from mlflow_provider.operators.registry import CreateRegisteredModelOperator
import mlflow

# Adjust these parameters
EXPERIMENT_ID = 0

## MLFlow parameters
MLFLOW_CONN_ID = "mlflow"
EXPERIMENT_NAME = "test_5"
REGISTERED_MODEL_NAME = "my_model"

mlflow.set_tracking_uri('http://host.docker.internal:5000')

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def mlflow_tutorial_dag():
    # 1. Use a hook from the MLFlow provider to interact with MLFlow within a TaskFlow task
    @task
    def create_experiment(experiment_name, **context):
        """Create a new MLFlow experiment with a specified name.
        Save artifacts to the specified S3 bucket."""

        # ts = context["ts"]

        mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
        new_experiment_information = mlflow_hook.run(
            endpoint="api/2.0/mlflow/experiments/create",
            request_params={
                "name": experiment_name
            },
        ).json()

        # print(new_experiment_information)

        return new_experiment_information['experiment_id']

    # 2. Use mlflow.sklearn autologging in a TaskFlow task
    @task
    def scale_features(experiment_id: str):
        """Track feature scaling by sklearn in Mlflow."""
        # from sklearn.datasets import fetch_california_housing
        # from sklearn.preprocessing import StandardScaler
        # import mlflow
        # import pandas as pd

        # df = fetch_california_housing(download_if_missing=True, as_frame=True).frame

        # mlflow.sklearn.autolog()

        # target = "MedHouseVal"
        # X = df.drop(target, axis=1)
        # y = df[target]

        # scaler = StandardScaler()
        # mlflow.set_experiment(EXPERIMENT_NAME)
        # experiment = mlflow.get_experiment_by_name("experiment name")

        with mlflow.start_run(experiment_id=experiment_id):
            # X = pd.DataFrame(scaler.fit_transform(X), columns=X.columns)
            # mlflow.sklearn.log_model(scaler, artifact_path="scaler")
            mlflow.log_param("precision", 1)



    # # 3. Use an operator from the MLFlow provider to interact with MLFlow directly
    # create_registered_model = CreateRegisteredModelOperator(
    #     task_id="create_registered_model",
    #     name="{{ ts }}" + "_" + REGISTERED_MODEL_NAME,
    #     tags=[
    #         {"key": "model_type", "value": "regression"},
    #         {"key": "data", "value": "housing"},
    #     ],
    # )

    # (
    #     create_experiment(
    #         experiment_name=EXPERIMENT_NAME
    #     )

        
    #     # >> create_registered_model
    # )
    experiment_created = create_experiment(
        experiment_name=EXPERIMENT_NAME
    )

    (
        experiment_created >> scale_features(experiment_id=experiment_created)
    )

mlflow_tutorial_dag()