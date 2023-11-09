import pandas as pd


import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import preprocess_data

import os

def query_data():
	conn = snow.connect(user=os.environ['SNOWFLAKE_USER'],
	password=os.environ['SNOWFLAKE_PASS'],
	account=os.environ['SNOWFLAKE_ACC'])
	cur = conn.cursor()

	cur.execute("USE WAREHOUSE TEST_WAREHOUSE")
	cur.execute("USE DATABASE FORECAST_DB")

	query = '''
	select * from FORECAST_DB.FORECAST_SCHEMA.FORECASTS_TB t1 JOIN \
	FORECAST_DB.FORECAST_SCHEMA.TIDES_TB t2 ON t1.TIMESTAMP_TIMESTAMP = t2.TIMESTAMP_TIMESTAMP AND t1.ID = t2.ID
	'''

	cur = conn.cursor().execute(query)
	df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])	
	return df

def query_processed():
	conn = snow.connect(user=os.environ['SNOWFLAKE_USER'],
	password=os.environ['SNOWFLAKE_PASS'],
	account=os.environ['SNOWFLAKE_ACC'])
	cur = conn.cursor()

	cur.execute("USE WAREHOUSE TEST_WAREHOUSE")
	cur.execute("USE DATABASE FORECAST_DB")

	query = '''
	select * from FORECAST_DB.FORECAST_SCHEMA.PROCESSED_TB
	'''

	cur = conn.cursor().execute(query)
	df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])	
	return df

def set_up():
	conn = snow.connect(user=os.environ['SNOWFLAKE_USER'],
	password=os.environ['SNOWFLAKE_PASS'],
	account=os.environ['SNOWFLAKE_ACC'])
	cur = conn.cursor()

	cur.execute("USE ROLE SYSADMIN")
	cur.execute("CREATE WAREHOUSE IF NOT EXISTS TEST_WAREHOUSE WITH WAREHOUSE_SIZE = XSMALL")
	cur.execute("DROP DATABASE IF EXISTS FORECAST_DB")
	cur.execute("CREATE DATABASE FORECAST_DB")
	cur.execute("USE DATABASE FORECAST_DB")
	cur.execute("CREATE SCHEMA IF NOT EXISTS FORECAST_SCHEMA")

	sql = "CREATE TABLE IF NOT EXISTS FORECASTS_TB_TEMP(id VARCHAR(50), timestamp_timestamp NUMERIC, timestamp_dt VARCHAR(50), weather_temperature NUMERIC, \
								weather_condition VARCHAR(50), wind_speed FLOAT(), wind_direction FLOAT(), surf_min FLOAT(), \
								surf_max FLOAT())"
	cur.execute(sql)

	sql = "CREATE TABLE IF NOT EXISTS FORECASTS_TB(id VARCHAR(50), timestamp_timestamp NUMERIC, timestamp_dt VARCHAR(50), weather_temperature NUMERIC, \
								weather_condition VARCHAR(50), wind_speed FLOAT(), wind_direction FLOAT(), surf_min FLOAT(), \
								surf_max FLOAT())"
	cur.execute(sql)
	cur.execute("ALTER TABLE FORECASTS_TB ADD CONSTRAINT unique_hour UNIQUE (id, timestamp_timestamp)");


	sql = "CREATE TABLE IF NOT EXISTS TIDES_TB_TEMP(id VARCHAR(50), timestamp_timestamp NUMERIC, timestamp_dt VARCHAR(50), type VARCHAR(20), HEIGHT FLOAT())"
	cur.execute(sql)

	sql = "CREATE TABLE IF NOT EXISTS TIDES_TB(id VARCHAR(50), timestamp_timestamp NUMERIC, timestamp_dt VARCHAR(50), type VARCHAR(20), HEIGHT FLOAT())"
	cur.execute(sql)
	cur.execute("ALTER TABLE TIDES_TB ADD CONSTRAINT unique_hour UNIQUE (id, timestamp_timestamp)");

	cur.close()
	conn.close()

def update_with_df(file, cols, conn, table_name):

	# Get it as a pandas dataframe.
	df = pd.read_csv(file, sep = ",")
	df = df[cols]
	col = df.pop("id")
	df.insert(0, col.name, col)

	df.columns = map(str.upper, df.columns)

	# # Actually write to the table in snowflake.

	write_pandas(conn=conn, df=df, table_name=f"{table_name}_TB_TEMP", overwrite=True)

	temp_str = ""
	for c in cols:
		temp_str += c + ", "

	temp_str = temp_str[:-2]

	# sql = f"INSERT INTO {table_name}_TB({temp_str}) \
	# SELECT * FROM {table_name}_TB_TEMP t \
	# WHERE NOT EXISTS (SELECT * FROM {table_name}_TB t2 WHERE t.id = t2.id AND t.timestamp_timestamp = t2.timestamp_timestamp)"

	sql = f"INSERT INTO {table_name}_TB({temp_str}) \
	SELECT * FROM {table_name}_TB_TEMP t \
	WHERE NOT EXISTS (SELECT * FROM {table_name}_TB t2 WHERE t.ID = t2.ID AND t.TIMESTAMP_TIMESTAMP = t2.TIMESTAMP_TIMESTAMP)"


	return sql
	
def update_db():
	conn = snow.connect(user=os.environ['SNOWFLAKE_USER'],
	password=os.environ['SNOWFLAKE_PASS'],
	account=os.environ['SNOWFLAKE_ACC'])
	cur = conn.cursor()

	cur.execute("USE ROLE SYSADMIN")
	cur.execute("USE WAREHOUSE TEST_WAREHOUSE")
	cur.execute("USE DATABASE FORECAST_DB")
	cur.execute("USE SCHEMA FORECAST_SCHEMA")
	dir = "csvs"
	csv_files = os.listdir(dir)

	# table_name = "FORECASTS"
	# cols = 

	for table_name, cols in zip(["FORECASTS", "TIDES"], 
								[['id', 'timestamp_timestamp', 'timestamp_dt', 'weather_temperature', 'weather_condition', 'wind_speed', 'wind_direction', 'surf_min', 'surf_max'],
								['id', 'timestamp_timestamp', 'timestamp_dt', 'type', 'height']]):

		for file in csv_files:
			if file.split("_")[0] == table_name.lower():
				sql = update_with_df(os.path.join(dir, file), cols, conn, table_name)
				cur.execute(sql)


	## Phase III: Turn off the warehouse.
	# Create a cursor object.
	cur = conn.cursor()

	# Execute a statement that will turn the warehouse off.
	# sql = "ALTER WAREHOUSE TEST_WAREHOUSE SUSPEND"
	# cur.execute(sql)

	# Close your cursor and your connection.
	cur.close()
	conn.close()

def preprocess_and_store():
	conn = snow.connect(user=os.environ['SNOWFLAKE_USER'],
	password=os.environ['SNOWFLAKE_PASS'],
	account=os.environ['SNOWFLAKE_ACC'])
	cur = conn.cursor()

	cur.execute("USE ROLE SYSADMIN")
	cur.execute("USE WAREHOUSE TEST_WAREHOUSE")
	cur.execute("USE DATABASE FORECAST_DB")
	cur.execute("USE SCHEMA FORECAST_SCHEMA")
	cur.execute("DROP TABLE IF EXISTS PROCESSED_TB")



	data = query_data()
	# print(len(data))
	processed_data = preprocess_data.encode_features(data)

	temp_str = ""
	for c in list(processed_data.columns):
		temp_str += c + " FLOAT(), "

	temp_str = temp_str[:-2]

	print(temp_str)

	cur.execute("DROP TABLE IF EXISTS PROCESSED_TB")
	sql = f"CREATE TABLE IF NOT EXISTS PROCESSED_TB({temp_str})"
	cur.execute(sql)

	processed_data.columns = map(str.upper, processed_data.columns)
	write_pandas(conn=conn, df=processed_data, table_name=f"PROCESSED_TB")

def test():
	conn = snow.connect(user=os.environ['SNOWFLAKE_USER'],
	password=os.environ['SNOWFLAKE_PASS'],
	account=os.environ['SNOWFLAKE_ACC'])
	cur = conn.cursor()

	cur.execute("USE WAREHOUSE TEST_WAREHOUSE")
	cur.execute("USE DATABASE FORECAST_DB")

	query = '''
	SELECT * FROM FORECAST_DB.FORECAST_SCHEMA.FORECASTS_TB ORDER BY TIMESTAMP_TIMESTAMP
	'''

	cur = conn.cursor().execute(query)
	df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])	
	print(df)
	return df

if __name__ == "__main__":
	# set_up()
	update_db()
	# preprocess_and_store()
	test()
