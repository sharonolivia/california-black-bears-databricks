"""
File: msds697_county.py
Function:   Helper code to assign county data to each observation in the
            bear api files being processed. Returns a Pandas dataframe of
            the processed api file data with county data added to it.
"""

# Import packages
from geopy.geocoders import Nominatim
import pandas as pd


def add_county(df):
    '''
    Add address to each observation and keep only the county in the address.
    Uses API call to Nominatim service via the geopy package.
    Return Pandas dataframe with county assigned to the observations.
    '''
    # Begin downloading address data from Nominatim's API through geopy.
    # Assign the county data to each observation in the dataframe.
    geolocator = Nominatim(user_agent="geoapiExercises")
    county = []
    geocoords = df[['latitude', 'longitude']].values
    for elems in geocoords:
        address, _ = geolocator.reverse((f'{elems[0]}, {elems[1]}'))
        address = address.split(',')

        for i in address:
            if 'county' in i.lower():
                county.append(i.strip())

    df = pd.concat([df, pd.Series(county, name='county')], axis=1)
    return df
