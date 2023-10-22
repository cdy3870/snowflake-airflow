import pandas as pd
from sklearn.preprocessing import StandardScaler
import snowflake.connector as snow
import snowflake_ingest as si

def standardize_data(X_train, X_test):
	scaler = StandardScaler()
	scaler = scaler.fit(X_train)
	X_train = scaler.transform(X_train)
	X_test = scaler.transform(X_test)

	return X_train, X_test

def adjust_categorical(data, feats):
	for f in feats:
		# data[f] = pd.get_dummies(data[f]) 
		data = pd.get_dummies(data, columns=[f], prefix='', prefix_sep='')
		# data = data.drop(f, axis=1)

	return data * 1

def encode_features(data):

	cols = [c.upper() for c in ['weather_temperature', 'weather_condition', 'wind_speed', 'wind_direction', 'surf_min', 'height']]
	categ = [c.upper() for c in ['weather_condition']]
   
	print(data)
	print(cols)
	data = data[cols]
	data = adjust_categorical(data, categ)    
	
	return data


if __name__ == "__main__":
	# feature_extract()
	print(si.query_data())
