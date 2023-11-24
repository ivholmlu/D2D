import streamsync as ss
import pandas as pd
import plotly.express as px

# Its name starts with _, so this function won't be exposed

# Import data


def _get_main_df():
    main_df = pd.read_csv('../../Data/summary_oneyear.csv')
    # get unique name and lat lon
    main_df = main_df.drop_duplicates(subset=['name'])
    main_df = main_df[['name', 'lat', 'lon']]
    main_df = main_df.reset_index(drop=True)
    return main_df

# Plot fishplant


def _update_plotly_fishplant(state):
    fishplant = state["fishplant_df"]
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
    state["selected"] = fishplant["name"].values[payload[0]["pointNumber"]]
    state["selected_num"] = payload[0]["pointNumber"]
    _update_plotly_fishplant(state)


# Initialise the state

# "_my_private_element" won't be serialised or sent to the frontend,
# because it starts with an underscore (not used here)
initial_state = ss.init_state({
    "my_app": {
        "title": "Restaurant selection"
    },
    "_my_private_element": 1337,
    "selected": "Click to select",
    "selected_num": -1,
    "fishplant_df": _get_main_df(),
})

_update_plotly_fishplant(initial_state)
