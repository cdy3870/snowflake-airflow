import pysurfline
import os
import shutil

spot_id = "5842041f4e65fad6a7708852" # anchor point

# spotforecasts = pysurfline.get_spot_forecasts(spotId)


class Forecast():
	def __init__(self, spot_id):
		self.spot_id = spot_id
		self.set_data()
		self.set_dfs()
		self.assign_ids()

	def assign_ids(self):
		for key, value in self.get_dfs().items():
			self.get_dfs()[key]["id"] = self.spot_id

	def set_data(self):
		self.data = pysurfline.get_spot_forecasts(self.spot_id)

	def get_data(self):
		return self.data

	def set_dfs(self):
		types = ["forecasts", "tides", "sunriseSunsetTimes"]
		self.df_mapping = {}
		for t in types:
			self.df_mapping[t] = self.data.get_dataframe(t)

	def get_dfs(self):
		return self.df_mapping

def save_csvs(**kwargs):
	dir = 'csvs'
	shutil.rmtree(dir, ignore_errors=True)
	os.makedirs(dir)

	for location, spot_id in kwargs["ids"].items():

		forecast = Forecast(spot_id)

		df_mapping = forecast.get_dfs()

		for key, df in df_mapping.items():
			df.to_csv(f"csvs/{key}_data_{location}.csv", encoding='utf-8', index=False)

# def save_csvs(spot_id):
# 	forecast = Forecast(spot_id)

# 	df_mapping = forecast.get_dfs()

# 	for key, df in df_mapping.items():
# 		df.to_csv(f"{key}_data.csv", encoding='utf-8', index=False)

if __name__ == "__main__":
	# forecast = Forecast(spot_id)
	# print(forecast.get_dfs())

	save_csvs({'Rockaways': "5842041f4e65fad6a7708852", "Malibu": "584204214e65fad6a7709b9f"})