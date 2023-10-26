import streamlit as st
import os

# Initialize connection.
conn = st.experimental_connection('snowpark')

# df = conn.query('SELECT * from FORECASTS_TB_TEMP;', ttl=600)
# df

# Perform query.
# df = conn.query('SELECT * from FORECASTS_TB;', ttl=600)
# tides_df = conn.query('SELECT * from TIDES_TB;', ttl=600)

# df = df.sort_values("TIMESTAMP_DT")

# df
# # print(df.iloc[0]["TIMESTAMP_DT"])
# left = df.iloc[0]["TIMESTAMP_DT"]
# right = df.iloc[-1]["TIMESTAMP_DT"]



# st.write(f"Num hours: {len(df)}")
# st.write(f"Time range: {left} - {right}")
# st.write(f"API calls made: {int(len(df)/25)}")


def find_best_model():
    mlruns = "airflow/dags/mlruns/0"
    runs = os.listdir(mlruns)
    min_avg = 1000
    best_run = ""

    for run in runs:
        if run not in [".DS_Store", "meta.yaml"]:
            with open(os.path.join(mlruns, run, "metrics/mae"), "r") as f:
                mae = f.readline().split(" ")[1]
            with open(os.path.join(mlruns, run, "metrics/r2"), "r") as f:
                r2 = f.readline().split(" ")[1]
            with open(os.path.join(mlruns, run, "metrics/rmse"), "r") as f:
                rmse = f.readline().split(" ")[1]

            if (mae + r2 + rmse)/3 < min_avg:
                best_run = run
    model_path = os.path.join(mlruns, best_run)
    return 

if __name__ == "__main__":
    find_best_model()