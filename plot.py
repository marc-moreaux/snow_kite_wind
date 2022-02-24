import os
import pygrib
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt


def get_all_grb_selection(grbs, name):
    ''' 
    Select a named category in the GRBS and return all values in a
    DataFrame[ Longitude, Latitude, selection_date1, selection_date2 ... ]
    '''
    df = pd.DataFrame()
    for grb in grbs:
        grb_selection = grb.select(name=name)
        for s in grb_selection:
            date = f'{s.date}_{s.hour + s.forecastTime:02d}'
            if df.empty:
                df = pd.DataFrame(
                    s.latLonValues.reshape((-1, 3)),
                    columns=['lat', 'lon', date])
            else:
                df[date] = s.latLonValues.reshape((-1, 3))[:, 2]
    return df


def restructure_df(df):
    df = df.set_index(['lon', 'lat'])
    new_df = pd.DataFrame()
    for key in df.keys():
        _df = df[key].reset_index().rename(columns={key: 'value'})
        _df['date'] = key
        new_df = new_df.append(_df)
    return new_df.reset_index()


# Open grib files
files = sorted([f for f in os.listdir('./runs/') if f.endswith('.grib2')])
grbs = [pygrib.open('./runs/' + f) for f in files]

df_wSpeed = get_all_grb_selection(grbs, '10 metre wind speed')
df_wDir = get_all_grb_selection(grbs, '10 metre wind direction')
df_temp = get_all_grb_selection(grbs, '2 metre temperature')

# Set Swiss longitute, latitude and desired wind
hour_min, hour_max = 9, 17
lon_min, lon_max = 6, 10
lat_min, lat_max = 46, 47
speed_min, speed_max = 1, 64
dir_min, dir_max = 0, 90

# Apply selection
df_wSpeed = a.copy()
df_wSpeed = df_wSpeed[ (df_wSpeed.lon > lon_min)
                     & (df_wSpeed.lon < lon_max)
                     & (df_wSpeed.lat > lat_min)
                     & (df_wSpeed.lat < lat_max)]

df_wSpeed = restructure_df(df_wSpeed)

df = df_wSpeed[(df_wSpeed.value > speed_min) & (df_wSpeed.value < speed_max)][::20]
# Plot
fig = px.scatter_mapbox(df_wSpeed, lat="lat", lon="lon", color='value', animation_frame='date',
                        color_continuous_scale=px.colors.cyclical.IceFire, zoom=4, opacity=.5)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()


store = pd.HDFStore("store.h5")

def append_df(grb_path):
    _, resolution, _, pred_type, _, pred_date, _ = grb_path.split('/')[-1].split('_')
    pred_date = pred_date.split(':')[0]
    grb = pygrib.open(grb_path)

    if 'wind_dir' not in store.keys():
        df = pd.DataFrame(
            grb.latLonValues.reshape((-1, 3)),
            columns=['lat', 'lon', pred_date])
        store['wind_dir'] = df

#ws = sqrt(u2+v2)
#wd = math.atan2(v,u)


files = sorted([f for f in os.listdir('./runs/')])
grbs = [pygrib.open('./runs/' + f) for f in files]
grb = grbs[0]
s = grb.select()[0] 
df = pd.DataFrame(
    s.latLonValues.reshape((-1, 3)),
    columns=['lat', 'lon', 'value']
)
df[df['value'] == 9999] = 280 
plt.imshow(df.value.values.reshape(1684, -1))
plt.show()
