import requests
import json
import pandas as pd
import numpy as np
import csv
import json

api_key = "NSH-FQ788GmfTTaoBbNLpSwLQGpBy6dGDa5lCf3pGuld3TBqJjKXXM-K8xGZ8evmc4woheT1PmEj_iC3UzN8aqB57lvxbzTbxGlL0ldC5EX-2y4zpN-HoFHCwc5aZHYx"
headers = {"Authorization": 'Bearer %s' % api_key}

def test_func():
	print("1")

def generate_x_searches(x):
	all_data = []
	count = 0
	while count < x:
		print(f"extracting {count}/{x}")
		url = f'https://api.yelp.com/v3/businesses/search?offset={count}'
		params = {'location':'Boston'}
		req = requests.get(url, params=params, headers=headers)

		data = json.loads(req.text)

		if "businesses" in data:
			for d in data["businesses"]:
				categories_string = ""
				for cat in d["categories"]:
					categories_string = categories_string  + ", " + cat["title"]

				try:
					all_data.append([d["name"], d["price"],
									 d["review_count"], d["rating"],
									 d["location"]["address1"], d["location"]["city"],
									 str(d["location"]["zip_code"]), d["location"]["country"],
									 d["location"]["state"], d["image_url"], categories_string[2:]])
				except:
					continue
		count += 20
	# print(data["businesses"][0].keys())




	return all_data


def explore_endpoints():
	url = f'https://api.yelp.com/v3/businesses/search'
	params = {'location':'Boston'}
	req = requests.get(url, params=params, headers=headers)
	data = json.loads(req.text)

	if "businesses" in data:
		for d in data["businesses"]:
			url = f'https://api.yelp.com/v3/businesses/engagement?business_ids={d["id"]}'
			print(url)
			req = requests.get(url, headers=headers)
			data = json.loads(req.text)

			print(data)

def explore_json_data():
	data = []
	count = 0
	with open('yelp_data/yelp_academic_dataset_user.json') as f:
		for line in f:
			print(line)
			count += 1
			data.append(json.loads(line))

			if count == 10:
				break
	print(data)

def run_create_csv():
	# explore_endpoints()
	# explore_json_data()


	all_data = np.transpose(np.array(generate_x_searches(5000)))

	data_dict = dict(zip(["name", "price", "review_count", "rating",
						  "address", "city", "zip_code", "country", "state",
						  "image_url", "categories"], all_data))

	df = pd.DataFrame(data_dict)
	return df.to_csv("yelp_data.csv", encoding='utf-8', index=False)

# if __name__ == "__main__":
# 	main()