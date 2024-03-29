from __future__ import annotations

import random
import time

import requests
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

# Scraping parameters
SCRAPE_PROXY = 'socks5://127.0.0.1:9050'
SCRAPE_RTD_MINIMUM = 1
SCRAPE_RTD_MAXIMUM = 5
SCRAPE_RETRIES_AMOUNT = 1
SCRAPE_RTD_ERROR_MINIMUM = 0.5
SCRAPE_RTD_ERROR_MAXIMUM = 1

#create object of SparkContext
spark = SparkSession.builder.master('local').\
    appName('Word Count').\
    config('spark.driver.bindAddress','localhost').\
    config('spark.ui.port','4050').\
    getOrCreate()

def sleep_timer(min=SCRAPE_RTD_MINIMUM, max=SCRAPE_RTD_MAXIMUM):
    time.sleep(random.uniform(min, max))  # RTD

def get_html(url):
    """
    Retrieves the HTML content given a Internet accessible URL.
    :param url: URL to retrieve.
    :return: HTML content formatted as String, None if there was an error.
    """
    for i in range(0, SCRAPE_RETRIES_AMOUNT):

        try:

            # Attempt to get url
            response = requests.get(url)

            # Check that the response worked
            assert response.ok

            # Extract content
            html_content = response.content
            return html_content

        except Exception as e:
            if i == SCRAPE_RETRIES_AMOUNT - 1:
                print(f'Unable to retrieve HTML from {url}: {e}')
            else:
                sleep_timer()
    return None

def run_parallel_calls(func, values, partitions=2):

    # Set schema
    schema = T.StructType([T.StructField('start', T.StringType())])

    # Create spark dataframe
    sdf = spark.createDataFrame([(i, ) for i in values if i is not None], schema=schema)
    sdf = sdf.repartition(partitions)

    # Run artist extraction in parallel
    udf = F.udf(func)
    sdf = sdf.withColumn('end', udf('start'))

    return sdf

def convert_str_to_json(sdf, col, json_schema='MAP<STRING,STRING>', explode=False):

    if explode:
        # Convert array of lists
        sdf = sdf.withColumn(col, F.split(col, ';'))
        sdf = sdf.select(F.explode(col).alias(col))

    # Expand json into columns
    sdf = sdf.withColumn(
        'x',
        F.from_json(col, json_schema)
    )

    # Get dictionary keys
    keys = (sdf
        .select(F.explode('x'))
        .select('key')
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Select final columns
    exprs = [F.col('x').getItem(k).alias(k) for k in keys]
    sdf = sdf.select(*exprs)

    return sdf


def get_spotify_api():

    # Setup  Spotify OAuth
    sp_oauth = SpotifyClientCredentials()

    # Initialize Spotify API
    sp = Spotify(auth_manager=sp_oauth, requests_timeout=1, retries=1, status_retries=1, requests_session=False)

    return sp
