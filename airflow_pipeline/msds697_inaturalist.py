"""
File: msds697_inaturalist_process.py
Function:   Checks Google Storage for new API data pulled from iNaturalist. The
            API data will be processed so only relevant fields are extracted.
"""
# Import packages
import json
import pyspark
from pyspark.sql import SparkSession as SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import *

# Initiate Spark context session
sc = pyspark.SparkContext()

# Initiate Spark session
ss = SparkSession.builder.getOrCreate()


def get_latest_file(storage_client, bucket_name):
    # Get a list of all the downloaded API responses in Google Storage.
    blobs = [(blob, blob.updated) for blob in storage_client.list_blobs(
        bucket_name, prefix='inaturalist_data/'
    )]

    # Get the latest file and return it for processing.
    return sorted(blobs, key=lambda tup: tup[1])[-1][0]


def load_json(blob_obj):
    '''
    Load a JSON file from Google Storage
    '''
    js = blob_obj.download_as_string()
    js = json.loads(js)
    return js


def json_to_aggregate(json_file):
    '''
    Take in a JSON document and extract only:
    observed_on,
    longitude,
    latitude
    If do_append=True, then append the new file set to old baseline set.
    Return the data as an aggregate which is ready for upload to MongoBD.
    '''
    # Load as a Spark dataframe.
    json_file = sc.parallelize([json_file])
    rawdf = ss.read.json(json_file)

    # Extract the observed date object from the JSON file.
    observed_date = rawdf.select('results.observed_on_details')
    observed_date = observed_date.select(explode('observed_on_details')
                                        .alias('observed_on_details'))

    # Extract the geocoordinates object from the the JSON file.
    geocoords = rawdf.select('results.geojson')
    geocoords = geocoords.select(explode('geojson').alias('geojson'))

    # Extract the dates, latitude and longitude data from the objects.
    coord = geocoords.select(col('geojson.coordinates')[1].alias('latitude'),
                            col('geojson.coordinates')[0].alias('longitude'))
    observed_date = observed_date.select(col('observed_on_details.date')
                                         .alias('observed_on'),
                                        col('observed_on_details.year')
                                         .alias('observed_year'),
                                        col('observed_on_details.month')
                                         .alias('observed_month'),
                                        col('observed_on_details.day')
                                         .alias('observed_day')
                                         )

    # Join the dataframes into one and return it.
    latest_inat_api = observed_date.join(coord).toPandas()
    return latest_inat_api
