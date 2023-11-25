from pyspark.sql import SparkSession
import streamsync as ss
import pandas as pd
import plotly.express as px

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


def _get_main_df():
    # main_df = pd.read_csv('../../Data/summary_oneyear.csv')
    main_df = df_fish = spark.read.format("org.apache.spark.sql.cassandra").options(
        table="fish_table_year", keyspace="fish_keyspace").load().toPandas()

    return main_df

# Plot fishplant


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
    fishplant = fishplant[['name', 'lat', 'lon']]
    fishplant = fishplant.reset_index(drop=True)

    state["selected"] = fishplant["name"].values[payload[0]["pointNumber"]]
    state["selected_num"] = payload[0]["pointNumber"]
    _update_plotly_fishplant(state)


def handle_choice(state, payload):
    fishplant = state["fishplant_df"]
    fishplant = fishplant.drop_duplicates(subset=['name'])
    fishplant = fishplant[['name']]
    fishplant = fishplant.reset_index(drop=True)

    state["selected"] = fishplant["name"].values[int(payload)]
    state["selected_num"] = int(payload)
    _update_plotly_fishplant(state)


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
})

_update_plotly_fishplant(initial_state)
_get_JSON(initial_state)
