from No_sync.Frost_api import client_id, client_secret
import requests
import pandas as pd
from datetime import datetime, timedelta


def get_closest_station(lat, lon, num_stations=5, client_id=client_id):
    # Get the closest station to the location

    url = f"https://frost.met.no/sources/v0.jsonld?geometry=nearest(POINT({lon}%20{lat}))&nearestmaxcount={num_stations}"
    response = requests.get(url, auth=(client_id, ''))
    if response.status_code == 200:
        json_data = response.json()
        # print(json_data)
        # print(json_data['data'][0]['id'])
        print('Datapoints for stations retrieved from frost.met.no!')
        # return a list of the id of the closest stations
        return [json_data['data'][i]['id'] for i in range(num_stations)]

    else:
        print("Error! Returned status code %s" % response.status_code)
        return None


def fetch_weather_data(year, client_id, lat, lon):

    df_station = get_closest_station(
        lat, lon, client_id=client_id, num_stations=5)
    # Define the endpoint and parameters
    endpoint = 'https://frost.met.no/observations/v0.jsonld'
    year = int(year)
    if year:
        # Calculate the date range for the specified year
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
    else:
        # Calculate the date range for the last year
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

    # Format the date range as a string
    date_range = start_date.strftime(
        '%Y-%m-%d') + '/' + end_date.strftime('%Y-%m-%d')

    # make a string of the station id's
    df_station = ','.join(df_station)

    parameters = {
        'sources': df_station,
        'elements': 'mean(air_temperature P1D),sum(precipitation_amount P1D),mean(wind_speed P1D),mean(relative_humidity P1D)',
        'referencetime': date_range,
    }

    # Issue an HTTP GET request
    r = requests.get(endpoint, params=parameters, auth=(client_id, ''))

    # Check if the request was successful
    if r.status_code == 200:
        # Extract JSON data
        json_data = r.json()
        print('Data retrieved from frost.met.no!')
        return json_data

    else:
        print(
            f"Failed to fetch data. Status code: {r.status_code}, need most likely more stations")
        return None

# Example usage:


def make_df(year, client_id, lat, lon):
    json = fetch_weather_data(year, client_id, lat, lon)
    data = json['data']

    df_total = pd.DataFrame()
    for i in range(len(data)):
        row = pd.DataFrame(data[i]['observations'])
        row['referenceTime'] = data[i]['referenceTime']
        row['sourceId'] = data[i]['sourceId']

        df_total = pd.concat([row, df_total])

    # These additional columns will be kept
    columns = ['sourceId', 'referenceTime',
               'elementId', 'value', 'unit', 'timeOffset']
    df = df_total[columns].copy()
    # Convert the time value to something Python understands
    df['referenceTime'] = pd.to_datetime(df['referenceTime'])

    print(df['elementId'].unique())

    df.columns = df.columns.str.lower()
    df.to_csv('Data/weather_test.csv', index=False)
    return df


def weather_format(year, client_id, lat, lon):

    df_weather = make_df(year, client_id, lat, lon)
    df_weather['referencetime'] = pd.to_datetime(df_weather['referencetime'])
    df_weather['week'] = df_weather['referencetime'].dt.isocalendar().week
    df_weather['year'] = df_weather['referencetime'].dt.isocalendar().year

    lice_count_table = pd.read_csv('Data/lice_counts.csv')
    lice_count_table = lice_count_table.dropna(axis=1, how='all')

    # combine year and week into one column in lice_count_table
    lice_count_table['year_week'] = lice_count_table['year'].astype(
        str) + '_' + lice_count_table['week'].astype(str)
    # combine the year and week columns to one column
    df_weather['year_week'] = df_weather['year'].astype(
        str) + '_' + df_weather['week'].astype(str)
    print('hei')
    # gruop by referencetime
    df_weather = df_weather.groupby(
        ['referencetime', 'elementid', 'week', 'year', 'unit', 'sourceid', 'year_week'])
    df_weather = df_weather['value'].mean()
    df_weather = df_weather.reset_index()

    # combine the year and week columns to one column
    return df_weather, lice_count_table


def join_dataframes(year, client_id, lat, lon):

    df_weather, lice_count_table = weather_format(year, client_id, lat, lon)

    print(df_weather['elementid'].unique())
    print(df_weather.shape)

    df_weather = df_weather.drop_duplicates(
        subset=['referencetime', 'week', 'year', 'year_week', 'elementid'])
    print(df_weather.shape)
    # Pivot the DataFrame based on the 'elementid' column
    pivoted_df = df_weather.pivot(
        index=['referencetime', 'week', 'year', 'year_week'], columns='elementid', values='value')

    print(pivoted_df.shape)
    # Reset the index to the default integer index
    pivoted_df = pivoted_df.reset_index()

    # gruop by referencetime
    pivoted_df = pivoted_df.groupby(
        ['referencetime', 'week', 'year', 'year_week'])

    pivoted_df = pivoted_df.mean().reset_index()

    # drop week, year and year_week
    pivoted_df = pivoted_df.drop(['week', 'year'], axis=1)

    # drop index
    pivoted_df = pivoted_df.reset_index(drop=True)

    # set referencetime as index
    pivoted_df = pivoted_df.set_index('referencetime')

    # print unique values in elementid
    print(pivoted_df.shape)
    # columnes to mean
    columns_to_mean = ['mean(air_temperature P1D)',
                       'mean(relative_humidity P1D)', 'mean(wind_speed P1D)']
    columns_to_sum = ['sum(precipitation_amount P1D)']

    df_weekly = pivoted_df[columns_to_mean].resample('W').mean()
    df_weekly['sum(precipitation_amount P1D)'] = pivoted_df[columns_to_sum].resample(
        'W').sum()

    df_weekly['week'] = pivoted_df['year_week']

    # drop if row includes nan
    df_weekly = df_weekly.dropna(axis=0, how='any')

    # reset index
    df_weekly = df_weekly.reset_index()
    lice_count_table = lice_count_table.reset_index()
    print(df_weekly.shape)
    print(lice_count_table.shape)

    print(df_weekly.head(2))
    print(lice_count_table.head(2))
    # inner join the two dataframes on week column in df_weekly and year_week column in lice_count_table
    combine_df = pd.merge(df_weekly, lice_count_table,
                          how='left', left_on='week', right_on='year_week')
    print(combine_df.shape)
    # keep referencetime	mean(air_temperature P1D)	mean(relative_humidity P1D)	mean(wind_speed P1D)	sum(precipitation_amount P1D) avgadultfemalelice	avgmobilelice	avgstationarylice	localityname
    columns_to_keep = ['referencetime', 'mean(air_temperature P1D)', 'mean(relative_humidity P1D)', 'mean(wind_speed P1D)',
                       'sum(precipitation_amount P1D)', 'avgadultfemalelice', 'avgmobilelice', 'avgstationarylice', 'seatemperature', 'localityname']
    combine_df = combine_df[columns_to_keep]

    combine_df.columns = combine_df.columns.str.lower()

    column_mapping = {
        'referencetime': 'referencetime',
        'mean(air_temperature p1d)': 'mean_air_temperature',
        'mean(relative_humidity p1d)': 'mean_relative_humidity',
        'mean(wind_speed p1d)': 'mean_wind_speed',
        'sum(precipitation_amount p1d)': 'sum_precipitation_amount',
        'avgadultfemalelice': 'avgadultfemalelice',
        'avgmobilelice': 'avgmobilelice',
        'avgstationarylice': 'avgstationarylice',
        'seatemperature': 'seatemperature',
        'localityname': 'localityname'
    }

    # Assuming df is your DataFrame
    combine_df = combine_df.rename(columns=column_mapping)
    # drop the first row
    combine_df = combine_df.iloc[1:]
    # save as csv
    print(combine_df.shape)
    combine_df.to_csv('Data/combine_df_MEGA.csv', index=False)

    return combine_df


# year = '2020'
# lat = '63.469217'
# lon = '7.8523'

# dataf = join_dataframes(year, client_id, lat, lon)
