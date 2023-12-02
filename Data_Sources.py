from No_sync.credentials import config
import requests
from No_sync.credentials import config
import requests
from pprint import pprint
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt


# Get an access token from the API
def get_token():

    if not config['client_id']:
        raise ValueError('client_id must be set in credentials.py')

    if not config['client_secret']:
        raise ValueError('client_secret must be set in credentials.py')

    req = requests.post(config['token_url'],
                        data={
        'grant_type': 'client_credentials',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'scope': 'api'
    },
        headers={'content-type': 'application/x-www-form-urlencoded'})

    req.raise_for_status()
    print('Token request successful')
    return req.json()


# Get the weekly summary for a given year and week

def get_week_summary(token, year, week):
    """
    Fetches the weekly summary data for a specified year and week.

    Args:
        token (dict): Access token information.
        year (int): The year for which to fetch the data.
        week (int): The week of the year for which to fetch the data.

    Returns:
        dict: JSON response containing the weekly summary data.
    """
    url = f"{config['api_base_url']}/v1/geodata/fishhealth/locality/{year}/{week}"
    headers = {
        'authorization': 'Bearer ' + token['access_token'],
        'content-type': 'application/json',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def make_df(weeksummary):
    """
    Create a DataFrame from the week summary
    """
    df_data = pd.DataFrame()

    if 'localities' in weeksummary:
        localities = weeksummary['localities']

        if localities:
            # Extract common information
            common_info = {
                'year': weeksummary['year'],
                'week': weeksummary['week']
            }

            # Extract and flatten the localities data into a list of dictionaries
            data_list = []
            for information in localities:
                data = {**common_info, **information}
                data_list.append(data)

            # Create the DataFrame directly from the list of dictionaries
            df_data = pd.DataFrame(data_list)

    return df_data


def weeks_of_the_year(year=None):
    """
    Create a DataFrame with the week number and year for each week of the year
    """
    # Get the current year and the week of the year
    if year == None:
        today = datetime.now()
        year = today.year
        day_of_year = today.strftime('%j')
        week_of_year = (int(day_of_year) - 1) // 7 + 1

        # Calculate the number of weeks left in the previous year
        weeks_left_last_year = 52 - week_of_year

        # Create DataFrames for the current year and the remaining weeks from the previous year
        week_df = pd.DataFrame(
            {'Week': range(1, week_of_year + 1), 'Year': [year] * week_of_year})
        week_df_last = pd.DataFrame({'Week': range(
            1, weeks_left_last_year + 1), 'Year': [year - 1] * weeks_left_last_year})

        # Concatenate the two DataFrames
        week_df = pd.concat([week_df_last, week_df], ignore_index=True)
        # add week_of_year to week where year is current year - 1
        week_df.loc[week_df['Year'] == year - 1, 'Week'] += week_of_year
    else:
        week_numbers = list(range(1, 53))

        # Create a DataFrame with a "Week" column containing the week numbers
        week_df = pd.DataFrame({'Week': week_numbers})

        # Add a "Year" column with the specified year
        week_df['Year'] = year

    return week_df


def get_year_data(year):
    token = get_token()
    weeks_df = weeks_of_the_year(year)
    """
    This function returns a dataframe with all the data from the weeks in weeks_df
    """
    teller = 0
    df_data = pd.DataFrame()
    for week, year in weeks_df.values:
        teller += 1
        # print(week, year)
        weeksummary = get_week_summary(token, year, week)
        # print(weeksummary)
        df_to_concat = make_df(weeksummary)
        # print(df_to_concat)
        frames = [df_data, df_to_concat]
        df_data = pd.concat(frames, ignore_index=True)
        # print(teller)
    return df_data


def format_data(year):

    summary_oneyear = get_year_data(year)
    # add date column
    summary_oneyear['date'] = pd.to_datetime(summary_oneyear['year'].astype(
        str) + '-W' + summary_oneyear['week'].astype(str) + '-1', format='%Y-W%U-%w')
    # add date column
    summary_oneyear['date'] = pd.to_datetime(summary_oneyear['year'].astype(
        str) + '-W' + summary_oneyear['week'].astype(str) + '-1', format='%Y-W%U-%w')
    # Convert all column names to lower case
    summary_oneyear.columns = summary_oneyear.columns.str.lower()
    # Save df as csv
    summary_oneyear.to_csv('Data/summary_oneyear.csv', index=False)

    print('Data saved to csv')

    return summary_oneyear


def get_detailed_week(token, year, week, localityId):
    """
    Get the weekly summary for a given year and week
    """
    url = f"{config['api_base_url']}/v1/geodata/fishhealth/locality/{localityId}/{year}/{week}"
    headers = {
        'authorization': 'Bearer ' + token['access_token'],
        'content-type': 'application/json',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def extract_columns_from_json(json_data, desired_columns=['avgAdultFemaleLice', 'avgMobileLice', 'avgStationaryLice', 'localityName', 'speciesList', 'seaTemperature']):
    """
    Extract specified columns from a JSON input and create a DataFrame.

    :param json_data: JSON data as a dictionary.
    :param desired_columns: List of column names to extract.
    :return: A DataFrame containing the extracted columns.
    """
    # Extract columns from 'localityWeek' if they exist
    locality_week_data = json_data.get('localityWeek', {})
    locality_week_columns = {col: locality_week_data.get(col) for col in [
        'avgAdultFemaleLice', 'avgMobileLice', 'avgStationaryLice', 'seaTemperature']}

    # Extract 'speciesList' from 'aquaCultureRegister' if it exists
    aqua_culture_register_data = json_data.get('aquaCultureRegister', {})
    species_list = aqua_culture_register_data.get('speciesList')

    # Combine the extracted columns into a single dictionary
    filtered_data = {
        'avgAdultFemaleLice': locality_week_columns.get('avgAdultFemaleLice'),
        'avgMobileLice': locality_week_columns.get('avgMobileLice'),
        'avgStationaryLice': locality_week_columns.get('avgStationaryLice'),
        'seaTemperature': locality_week_columns.get('seaTemperature'),
        'localityName': json_data.get('localityName'),
        'speciesList': species_list
    }

    # Convert the dictionary into a DataFrame
    df = pd.DataFrame([filtered_data])

    return df


def get_lice_counts(token, aar, locality):
    """
    Get the lice counts for a given locality and a list of weeks.
    """
    weeks_df = weeks_of_the_year(aar)
    lice_counts = pd.DataFrame()
    for week, year in weeks_df.values:
        weekdetails = get_detailed_week(token, year, week, locality)

        # Extract the desired columns from the JSON data
        df = extract_columns_from_json(weekdetails)

        # Create a new column combining "year" and "week" as a string
        df['year_week'] = f"{year}-{week}"

        # Convert the "year_week" column to datetime format
        df['year_week'] = pd.to_datetime(
            df['year_week'] + '-1', format='%Y-%U-%w')

        # Keep the original "week" and "year" columns
        df['week'] = week
        df['year'] = year

        # Concatenate the DataFrames
        lice_counts = pd.concat([lice_counts, df], ignore_index=True)
    return lice_counts


def format_lice(aar, localityname, data):
    # Assuming you have a function called get_token defined elsewhere
    token = get_token()

    # Get localityNo from data for the given locality
    data_filtered = data[data['name'] == localityname]

    if not data_filtered.empty:  # Check if there are rows matching the condition
        locality_value = data_filtered['localityno'].values[0]

        lice_counts = get_lice_counts(token, aar, locality_value)
        lice_counts.columns = lice_counts.columns.str.lower()
        lice_counts.to_csv('Data/lice_counts.csv', index=False)
        print('Lice data saved to csv')
        return lice_counts
    else:
        print(f"No data found for locality: {localityname}")


# year = 2021
# locality = "Andholmen 1"
# data_1 = format_data(year)
# format_lice(year, locality, data_1)
