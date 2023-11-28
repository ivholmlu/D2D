from pyspark.sql import SparkSession
import streamsync as ss
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
# from ../../Data_Sources.ipynb import get_year_data()
"""
# Its name starts with _, so this function won't be exposed

# Import data
import os
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-20"
# If you are using environments in Python, you can set the environment variables like this:
# or similar to "/Users/kristian/miniforge3/envs/tf_M1/bin/python"
os.environ["PYSPARK_PYTHON"] = "python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "python" # or similar to "/Users/kristian/miniforge3/envs/tf_M1/bin/python"
# Set the Hadoop version to the one you are using, e.g., none:
os.environ["PYSPARK_HADOOP_VERSION"] = "without"

spark = SparkSession.builder.appName('SparkCassandraApp').\
    config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1').\
    config('spark.cassandra.connection.host', 'localhost').\
    config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions').\
    config('spark.sql.catalog.mycatalog', 'com.datastax.spark.connector.datasource.CassandraCatalog').\
    config('spark.cassandra.connection.port', '9042').getOrCreate()
# Some warnings are to be expected.
"""


def _get_main_df():
    main_df = pd.read_csv('../../Data/summary_oneyear.csv')
    """
    main_df = df_fish = spark.read.format("org.apache.spark.sql.cassandra").options(
        table="fish_table_year", keyspace="fish_keyspace").load().toPandas()
    """
    return main_df


def _get_lice_df():
    lice_df = pd.read_csv('../../Data/combine_df.csv')
    """
    lice_df = df_fish = spark.read.format("org.apache.spark.sql.cassandra").options(
        table="lice_table", keyspace="fish_keyspace").load().toPandas()
    """
    return lice_df

# Plot fishplant


def _model_predction(state):

    lice = state["lice_df"]
    selected_lice = state["selected_lice"]
    type_lice = ['avgadultfemalelice', 'avgmobilelice', 'avgstationarylice']

    if selected_lice in type_lice:
        from statsmodels.tsa.arima.model import ARIMA

        import statsmodels.api as sm

        mod = sm.tsa.statespace.SARIMAX(lice[selected_lice],
                                        lice[["seatemperature", "mean_air_temperature", "mean_relative_humidity",
                                              "mean_wind_speed", "sum_precipitation_amount"]],
                                        order=(1, 1, 1), seasonal_order=(0, 0, 0, 52), trend='c')
        res = mod.fit(disp=False)

        message = str(res.summary())
    else:
        message = "Choose lice type"

    state["message"] = message


def _update_plotly_lice(state):
    lice = state["lice_df"]
    selected_lice = state["selected_lice"]
    import random
    import plotly.graph_objs as go  # Import go from Plotly for adding red lines

    data = {
        'mean_air_temperature': 20,
        'mean_relative_humidity': 100,
        'mean_wind_speed': 15,
        'sum_precipitation_amount': 130,
        'avgadultfemalelice': 0.9,
        'avgmobilelice': 3,
        'avgstationarylice': 0.7,
        'seatemperature': 17,
    }
    values = []

    if selected_lice != "Choose lice type":
        lice = lice[['referencetime', selected_lice]]
        # Create a line plot using Plotly Express
        fig_lice = px.line(lice, x='referencetime', y=selected_lice)

        # Get the first and last values along the x-axis
        x_values = lice['referencetime']
        first_x = x_values.iloc[0]
        last_x = x_values.iloc[-1]

        # Create a red line trace from the first value to the last value along the x-axis
        red_line_trace = go.Scatter(
            x=[first_x, last_x],  # Span from the first to the last value along x
            # You can customize the y-coordinates as needed
            y=[data[selected_lice], data[selected_lice]],
            mode='lines',
            line=dict(color='red'),
            name='Red Line'
        )

        # Add the red line trace to the figure
        fig_lice.add_trace(red_line_trace)

        # Check if there are values higher than the red line
        values = lice[selected_lice]
        too_high = any(value > data[selected_lice] for value in values)

        
        if len(values) < 51:
            state['missing'] = True

        # Set the 'too_high' flag in the state
        state["too_high"] = too_high
        state["plotly_lice"] = fig_lice


def _update_plot_overtime(state):
    fishplant = state["fishplant_df"]
    selected_columns = state["selected_columns"]

    if selected_columns != "all":

        # if column type is boolean, get the proportion of True
        if fishplant[selected_columns].dtype == bool:
            # Group by 'date' and calculate the mean for numeric columns
            fishplant = fishplant.groupby(['date']).mean(
                numeric_only=True).reset_index()

        # Assuming 'name' and 'date' are columns in your DataFrame
        fishplant = fishplant[['date'] + [selected_columns]]
        # Create a line plot using Plotly Express
        fig_overtime = px.line(fishplant, x='date', y=selected_columns)

        state["plot_overtime"] = fig_overtime


def _update_plotly_fishplant_pie(state):
    fishplant = state["fishplant_df"]
    selected = state["selected_plant"]

    if selected != "all":
        fishplant = fishplant[fishplant['name'] == selected]
        selected_data = fishplant
    else:
        selected_data = fishplant
        print('hei')

    # Calculate the counts for 'haspd' column
    value_counts = selected_data['haspd'].value_counts()

    # Create a pie chart using Plotly Express
    fig_fishplant_pie = px.pie(
        names=value_counts.index,
        values=value_counts.values,
        title='Proportion of localities reporting Pancreas Disease (PD/Pd)',
    )

    # Assign the pie chart to state
    state["plotly_fishplant_pie"] = fig_fishplant_pie


def _update_plotly_fishplant(state):
    fishplant = state["fishplant_df"]

    # get unique name and lat lon
    fishplant = fishplant.drop_duplicates(subset=['name'])
    fishplant = fishplant[['name', 'lat', 'lon']]
    fishplant = fishplant.reset_index(drop=True)

    selected_num = state["selected_num"]
    sizes = [10]*len(fishplant)
    if selected_num != -1:
        sizes[selected_num] = 20
    fig_fishplant = px.scatter_mapbox(
        fishplant,
        lat="lat",
        lon="lon",
        hover_name="name",
        hover_data=["lat", "lon"],
        color_discrete_sequence=["darkgreen"],
        zoom=4,
        height=1000,
        width=700,
    )
    overlay = fig_fishplant['data'][0]
    overlay['marker']['size'] = sizes
    fig_fishplant.update_layout(mapbox_style="open-street-map")
    fig_fishplant.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    state["plotly_fishplant"] = fig_fishplant


def handle_click(state, payload):
    fishplant = state["fishplant_df"]

    fishplant = fishplant.drop_duplicates(subset=['name'])
    fishplant = fishplant[['name', 'lat', 'lon', 'haspd']]
    fishplant = fishplant.reset_index(drop=True)

    state["selected"] = fishplant["name"].values[payload[0]["pointNumber"]]
    state["selected_num"] = payload[0]["pointNumber"]
    state["selected_plant"] = state["selected"]

    _update_plotly_fishplant(state)
    _update_plotly_fishplant_pie(state)


def handle_choice(state, payload):
    fishplant = state["fishplant_df"]
    fishplant = fishplant.drop_duplicates(subset=['name'])
    fishplant = fishplant[['name']]
    fishplant = fishplant.reset_index(drop=True)

    state["selected"] = fishplant["name"].values[int(payload)]
    state["selected_num"] = int(payload)
    state["selected_plant"] = state["selected"]

    _update_plotly_fishplant(state)
    _update_plotly_fishplant_pie(state)


def handle_columns(state, payload):
    fishplant = state["fishplant_df"]
    columns = fishplant.columns
    columns = columns.drop(
        ['week', 'year', 'localityno', 'localityweekid', 'name', 'municipality', 'municipalityno', 'lat', 'lon', 'date'])

    state["selected_columns"] = columns.values[int(payload)]
    state["selected_columns_num"] = int(payload)

    _update_plot_overtime(state)


def get_lice(state, payload):

    lice = state["lice_df"]
    columns = lice.columns
    print(columns)

    state["selected_lice"] = columns.values[int(payload)]
    state["selected_lice_num"] = int(payload)

    _update_plotly_lice(state)
    _model_predction(state)


def _get_JSON(state):
    fishplant = state["fishplant_df"]
    # Create JSON with keys list(range(9)), and restaurant names as values
    fishplant = fishplant.drop_duplicates(subset=['name'])
    fishplant = fishplant[['name']]
    fishplant = fishplant.reset_index(drop=True)
    # sort alphabetically
    fishplant = fishplant.sort_values(by=['name'])

    my_json = dict(zip(list(range(len(fishplant))), fishplant["name"].values))
    # Convert keys to strings
    my_json = {str(key): value for key, value in my_json.items()}
    state["fishplant_JSON"] = my_json


def _get_JSON_col(state):
    fishplant = state["fishplant_df"]
    columns = fishplant.columns

    columns = columns.drop(
        ['week', 'year', 'localityno', 'localityweekid', 'name', 'municipality', 'municipalityno', 'lat', 'lon', 'date'])

    my_json = dict(zip(list(range(len(columns))), columns.values))
    # Convert keys to strings
    my_json = {str(key): value for key, value in my_json.items()}
    state["columns_JSON"] = my_json


def _get_JSON_licetype(state):
    lice = state["lice_df"]
    columns = lice.columns

    my_json = dict(zip(list(range(len(columns))), columns.values))
    # Convert keys to strings
    my_json = {str(key): value for key, value in my_json.items()}
    state["lice_JSON"] = my_json


# Initialise the state
# "_my_private_element" won't be serialised or sent to the frontend,
# because it starts with an underscore (not used here)
initial_state = ss.init_state({
    "my_app": {
        "title": "Fishplant selection"
    },
    "_my_private_element": 1337,
    "selected": "Click to select",
    "selected_num": -1,
    "fishplant_df": _get_main_df(),
    "selected_plant": "all",
    "selected_columns": "all",
    "selected_columns_num": -1,
    "selected_lice": "Choose lice type",
    "selected_lice_num": -1,
    "lice_df": _get_lice_df(),
    "message": None,
    "too_high": False,
    "missing": False,
})

_update_plotly_fishplant(initial_state)
_get_JSON(initial_state)
_update_plotly_fishplant_pie(initial_state)
_get_JSON_col(initial_state)
_update_plot_overtime(initial_state)
_get_JSON_licetype(initial_state)
_update_plotly_lice(initial_state)
_model_predction(initial_state)
