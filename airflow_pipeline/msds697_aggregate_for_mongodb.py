"""
File: msds697_aggregate_for_mongodb.py
Function:   Takes in the processed API file and converts it into aggregated
            format (for loading to MongoDB Atlas)
"""

# Import packages
import pandas as pd


def to_aggregate(df):
    '''
    Aggregate the data for uploading to MongoDB.
    Returns an aggregate dataset.
    '''
    # Construct geocoordinates aggregate array
    df['date_array'] = df.apply(lambda x: {'day': x['observed_day'],
                                           'month': x['observed_month'],
                                           'year': x['observed_year']}, axis=1)
    df['geocoords_agg'] = df.apply(lambda x: [x['longitude'],
                                              x['latitude']], axis=1)
    df['main_agg'] = df.apply(lambda x: {'observed_period':
                                         {'date': x['observed_on'],
                                          'date_unit': x['date_array']},
                                         'coords': x['geocoords_agg'],
                                         'county': x['county']}, axis=1)

    # Put dictionaries into a list for conversion to JSON.
    final = pd.Series(df['main_agg'])
    return final
