# Create a df to describe the files
descr = []
for item in grbs:
    item.seek(0)
    for grb in item:
        descr.append(str(grb).split(':'))
df = pd.DataFrame(
    descr, 
    columns=['id', 'name', 'unit', 'spacing', 'layer', 'level', 'hour', 'run_dt'])

# Get the grid
grb = grbs[0][54]
lats, lons = grb.latlons()  # WGS84 projection
lats.shape, lats.min(), lats.max(), lons.shape, lons.min(), lons.max()
shape = lats.shape
x = np.linspace(lons.min(), lons.max(), shape[1])
y = np.linspace(lats.min(), lats.max(), shape[0])
X, Y = np.meshgrid(x, y)

# Figure parameters
ratio = (lats.max() - lats.min()) / (lons.max() - lons.min()) 
size_int = 20
fig_size = (size_int, int(round(ratio * size_int)))

temp_min, temp_max = int(np.floor(np.min(temperatures))), int(np.ceil(np.max(temperatures)))
print(f'min {temp_min} / max {temp_max}')

# World map
world = gpd.read_file('./countries/ne_10m_admin_0_countries.shp')
ax = world.plot(color='white', edgecolor='black')

p1 = Point(lons.min(), lats.min())
p2 = Point(lons.max(), lats.min())
p3 = Point(lons.max(), lats.max())
p4 = Point(lons.min(), lats.max())
np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])

# Build France base figure
bb_polygon = Polygon([np1, np2, np3, np4])
bbox = gpd.GeoDataFrame(geometry=[bb_polygon])
france = gpd.overlay(world, bbox, how='intersection')
france.plot(color='white', edgecolor='black')

# Overlay temperatures
creation_time = datetime.datetime.strptime(run_time, "%Y-%m-%dT%H:%M:%S")
movie = False
for k in range(temperatures.shape[2]):
    fig, ax = plt.subplots(figsize=fig_size)
    CS = ax.contourf(X, Y, temperatures[:,:,k], levels=np.arange(temp_min, temp_max + 1), cmap='jet')
    france.geometry.boundary.plot(ax=ax, color=None, edgecolor='k',linewidth=2, alpha=0.25)
    reference_time = creation_time + datetime.timedelta(hours=k)
    ax.set_title(f"run {creation_time.isoformat()} - ref {reference_time.isoformat()}")
    cbar = fig.colorbar(CS)
    cbar.ax.set_ylabel('2 metre temperature (°C)')

# make an array of temperatures
arrays = []
for grb in grbs[:-1]:
    grb_hours = grb.select(name='10 metre wind speed') # , 10 metre wind direction
    for idx in range(len(grb_hours)):
        grb = grb_hours[idx]
        temper = np.copy(grb.values[::-1,])  # revert first axis
        temper -= 273.15  # from Kelvin to Celsius
        arrays.append(temper)
temperatures = np.stack(arrays, axis=2)


fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

df = pd.DataFrame(
    grb.latLonValues.reshape((-1,3)),
    columns=['lat', 'lon', 'temp'])
df.temp -= 273.15

fig = px.scatter_mapbox(df, lat="lat", lon="lon", hover_name="temp",
                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
fig = px.density_mapbox(df, lat="lat", lon="lon", z="temp", zoom=3, opacity=.5)
