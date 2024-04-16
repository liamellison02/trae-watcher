import logging
import sys
import requests
import json
import pandas as pd
import numpy as np

from config import config
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serializing_producer import SerializingProducer

base_url = "https://www.reddit.com"
endpoint = "/r/AtlantaHawks"
request_category = "/search.json"
specific_keys = ['name', 'subreddit_id', 'author', 'author_fullname', 'created_utc', 'title', 'ups', 'score',
                 'num_comments', 'total_awards_received', 'upvote_ratio', 'permalink']


def get_url(query, limit, time, sort):
    url = base_url + endpoint + request_category + "?" + f"q={query}" + f"&limit={limit}" + f"&t={time}" + f"&sort={sort}" + "&restrict_sr=on"
    return url


def get_search_results(url):
    try:
        res = requests.get(url)
        return res.json()
    except Exception as e:
        logging.info(e)


def clean_data(json_data: dict):
    return pd.Series([[listing['data'][key] for key in specific_keys] for listing in json_data['data']['children']], index=specific_keys)


def on_delivery(err, record):
    pass


def main():
    logging.info("START")

    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    reddit_listings_schema = schema_registry_client.get_latest_version("reddit_listings")

    kafka_config = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer": JSONSerializer(schema_registry_client, reddit_listings_schema.schema.schema_str)
    }

    producer = SerializingProducer(kafka_config)

    res = get_search_results(get_url("trae+young", "100", "year", "new"))
    df = clean_data(res)
    identifiers = df.loc[:, 'name']
    for name in identifiers:
        producer.produce(
            topic="reddit_listings",
            key=name,
            value={
                f"": ""

            },
            on_delivery=on_delivery
        )

    logging.debug(f"Got {clean_data(res)}")
    producer.flush()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())
