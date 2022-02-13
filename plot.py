import os
import pygrib
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
import plotly.express as px


def get_all_grb_selection(grbs, name, single_column=False):
    ''' 
    Select a named category in the GRBS and return all values in a
    DataFrame[ Longitude, Latitude, selection_date1, selection_date2 ... ]
    '''
    df = pd.DataFrame()
    for grb in grbs:
        grb_selection = grb.select(name=name)
        for s in grb_selection:
            date = f'{s.date}_{s.hour + s.forecastTime:02d}'
            if single_column:
                _df = pd.DataFrame(
                    s.latLonValues.reshape((-1, 3)),
                    columns=['lat', 'lon', 'value'])
                _df['date'] = date
                df = df.append(_df)
            else:
                if df.empty:
                    df = pd.DataFrame(
                        s.latLonValues.reshape((-1, 3)),
                        columns=['lat', 'lon', date])
                else:
                    df[date] = s.latLonValues.reshape((-1, 3))[:, 2]
    return df

# Open grib files
files = sorted([f for f in os.listdir('./runs/') if f.endswith('.grib2')])
grbs = [pygrib.open('./runs/' + f) for f in files]

df_wSpeed = get_all_grb_selection(grbs, '10 metre wind speed', True)
df_wDir = get_all_grb_selection(grbs, '10 metre wind direction', True)
df_temp = get_all_grb_selection(grbs, '2 metre temperature', True)

# Select desired wind
# Swiss longitute, latitude
lon_min, lon_max = 6, 10
lat_min, lat_max = 47, 46
speed_min, speed_max = 1, 64
dir_min, dir_max = 0, 90

df = df_wSpeed[(df_wSpeed.value > speed_min) & (df_wSpeed.value < speed_max)][::20]
# Plot
fig = px.scatter_mapbox(df, lat="lat", lon="lon", color='value', animation_frame='date',
                        color_continuous_scale=px.colors.cyclical.IceFire, zoom=4, opacity=.5)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()