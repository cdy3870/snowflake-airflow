# Research Conference Recommender

This repo contains the code for a data pipeline on surfing data. The purpose is to have a model that is updated daily with new surf weather information to determine if conditions are ideal. The predictive model is simple regressor at this point that predicts the height of tides at any given hour given forecast information. 

## Setup

1. Everything is done locally in an airflow container. Results and analytics will eventually be shown in a Streamlit dashboard.

## Libraries, Frameworks, APIs, Cloud Services
1. Libraries and Frameworks
- Airflow
- Docker
- MLFlow
2. APIs
- Surfline (via pysurfline package)
3. Databases/Warehouse
- Snowflake

## How it works and services involved
The following steps are automated via Airflow and set up to run daily except step 1, which only needs to be run once.

### Step 1: Set up Snowflake database, schema, tables
1. Using snowflake connectors, set up the warehouse with SQL commands
2. Create main tables for full forecast, tide, etc. info and temp tables to store current day data
   
### Step 2: Save current surfline data as csvs via pysurfline API
1. Pull the current day data from the API for a particular anchor point, which includes forecast, tide, and sunrise/sunset information
2. Store as three separate csvs with an additional id column
   
### Step 3: Update database and preprocess
1. Write current day csvs to temp tables
2. Insert into main tables if the current hour info does not already exist
3. Join forecast and tide information
4. Preprocess (extracted desired features, converting categorical variables)
5. Store processed data as a new table
   
### Step 3: Pull processed data, execute model, track results
1. Query processed data
2. Standardize data and create train-test splits
3. Train and evaluate regression model
4. Track updated model on MLFLow
