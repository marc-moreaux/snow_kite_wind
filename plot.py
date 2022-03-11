import matplotlib.pyplot as plt
import plotly.express as px
import pandas as pd
import h5py


with h5py.File('winds.h5','a') as h5f:
    temps = h5f['temp'][:] - 273.15

fig = px.imshow(temps, animation_frame=0)
fig.show()
