import logging
import sys
import requests
import json
import pandas as pd
import numpy as np


base_url = "https://www.reddit.com"
endpoint = "/r/AtlantaHawks"
request_category = "/search.json"
specific_keys = ['name', 'subreddit_id', 'author', 'author_fullname', 'created_utc', 'title', 'ups', 'score', 'num_comments', 'total_awards_received', 'upvote_ratio', 'permalink']

def get_url(query, limit, time, sort):
	url = base_url + endpoint + request_category + "?" + f"q={query}" + f"&limit={limit}"+ f"&t={time}"+ f"&sort={sort}"+ "&restrict_sr=on"
	return url
	
	
def get_search_results(url):
	try:
		res = requests.get(url)
		return res.json()
	except Exception as e:
		logging.info(e)


def clean_data(json_data):
	data = json_data['data']['children']
	df_rows = []

	for item in data:
		item_data = item['data']
		row = [item_data.get(key, np.nan) for key in specific_keys]
		df_rows.append(row)

	return pd.DataFrame(df_rows, columns=specific_keys)


def on_delivery(err, record):
	pass


def main():
	logging.info("START")

	kafka_config = config["kafka"]
	producer = SerializingProducer(kafka_config)

	res = get_search_results(get_url("trae+young", "100", "year", "new"))
	df = clean_data(res)
	identifiers = df.loc[:, 'name']
	for id in identifiers:
		producer.produce(
			topic="reddit_listings",
			key=id,
			value=value,
			on_delivery=on_delivery
			)

	logging.debug(f"Got {clean_data(res)}")


if __name__ == "__main__":
	logging.basicConfig(level=logging.DEBUG)
	sys.exit(main())