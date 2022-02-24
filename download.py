'''
Download wind and temperature data from meteoFrance API
https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=131&id_rubrique=51
Restrict to alps
lon_min, lon_max = 5, 11
lat_min, lat_max = 44, 47
'''
import os
import h5py
import numpy as np
from datetime import datetime, timedelta
import pygrib
import requests


def append_component(h5f, component, arr):
    if component not in h5f.keys():
        shape = (None, *arr.shape[1:])
        h5f.create_dataset(
            component, data=arr, compression="gzip", chunks=True, maxshape=shape)
    else:
        h5f[component].resize((h5f[component].shape[0] + arr.shape[0]), axis=0)


class Downloader():
    def __init__(self):
        # Define elements to download
        self.apiKey = 'eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJtYXJjLm1vcmVhdXhAY2FyYm9uLnN1cGVyIiwiYXBwbGljYXRpb24iOnsib3duZXIiOiJtYXJjLm1vcmVhdXgiLCJ0aWVyUXVvdGFUeXBlIjpudWxsLCJ0aWVyIjoiVW5saW1pdGVkIiwibmFtZSI6ImtpdGVfd2luZF9maW5kZXIiLCJpZCI6NzAzLCJ1dWlkIjoiNDAzZGJlN2YtMzJhOS00YzdiLTkzNjUtMTIzMzA2NDY1ZTRhIn0sImlzcyI6Imh0dHBzOlwvXC9wb3J0YWlsLWFwaS5tZXRlb2ZyYW5jZS5mcjo0NDNcL29hdXRoMlwvdG9rZW4iLCJ0aWVySW5mbyI6eyI1MFBlck1pbiI6eyJ0aWVyUXVvdGFUeXBlIjoicmVxdWVzdENvdW50IiwiZ3JhcGhRTE1heENvbXBsZXhpdHkiOjAsImdyYXBoUUxNYXhEZXB0aCI6MCwic3RvcE9uUXVvdGFSZWFjaCI6dHJ1ZSwic3Bpa2VBcnJlc3RMaW1pdCI6MCwic3Bpa2VBcnJlc3RVbml0Ijoic2VjIn19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInBlcm1pdHRlZFJlZmVyZXIiOiIiLCJzdWJzY3JpYmVkQVBJcyI6W3sic3Vic2NyaWJlclRlbmFudERvbWFpbiI6ImNhcmJvbi5zdXBlciIsIm5hbWUiOiJBUk9NRSIsImNvbnRleHQiOiJcL3B1YmxpY1wvYXJvbWVcLzEuMCIsInB1Ymxpc2hlciI6ImFkbWluX21mIiwidmVyc2lvbiI6IjEuMCIsInN1YnNjcmlwdGlvblRpZXIiOiI1MFBlck1pbiJ9XSwiZXhwIjoxNzM5NDYyODI4LCJwZXJtaXR0ZWRJUCI6IiIsImlhdCI6MTY0NDg1NDgyOCwianRpIjoiMmM2MGIxNjQtMTQwZC00MmIyLWE5YzMtMjE0YWQ2N2VkODBlIn0=.tv9VeD0Pn89Gx4eAnKxaFZmRYP1e_wwlu63DVPqGJU0Icwbntcl2vMuGZ01fEMwy38LhGbeCNKC3Nf_BHE7zDP3iSIoxjmeUKU4vIeL2FOyy3af5gZlHETqACgupKVIQ2IduW2b7m-VpMbqZXdYXk4L8l_W7Xuyyz-6ilzCeAj5JJfePbR-Vq-aRovRcKryMk7skqpP5Nq54k882aVAl0e4XTPxH8KQEAw8CpxtJVPqUefOcIyErEsf9usW7RzzQCsuODWMobRX5iOO4h9igKvGX1NhWW8Ay96blAQqbKHgOaTeGZ2QjK-DtTQZnpQJ8hI33jgdmQfyATf0NGMmdGw=='
        self.keys, self.elevations, self.short_keys = zip(*[
            ('TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND', 2, 'temp'),
            ('V_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND', 10, 'wind_v'),
            ('U_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND', 10, 'wind_u'),
            ('GEOMETRIC_HEIGHT__GROUND_OR_WATER_SURFACE', None, 'ground') ])

        # DL paths
        self.dir_path = os.path.join(os.getcwd(), 'runs')
        os.makedirs(self.dir_path, exist_ok=True)

    def get_last_run_date(self, offset=0):
        '''
        The runs are every 3 hours, from 00h to 21h
        last run full on info is the run before (now - 3h * offset)
        '''
        dt = datetime.now()
        dt = datetime(dt.year, dt.month, dt.day, dt.hour // 3 * 3)
        dt -= timedelta(hours=3 * (offset + 1))
        return dt.isoformat() + 'Z'

    def get_next_n_dates(self, date, n_dates=3):
        '''Get the next n_dates from date
        example:
            date = '2022-02-13T12:00:00Z'
            n_date = 3
        returns:
            ['2022-02-13T12:00:00Z',
            '2022-02-13T13:00:00Z',
            '2022-02-13T14:00:00Z',
            '2022-02-13T15:00:00Z']
        
        '''
        dt = datetime.fromisoformat(date[:-1])
        return [(dt + timedelta(hours=i)).isoformat() + 'Z'
                for i in range(n_dates + 1)]

    def get_run_and_pred_dates(self, offset=0, n_dates=2):
        '''Return a group of 3 dates that I want to DL'''
        return self.get_next_n_dates(
            self.get_last_run_date(offset),
            n_dates)

    def get_url_and_fName(self, key, elevation, resolution, run_date, pred_date, 
                          lats='(44.0,48.0)', lons='(5.0,13.0)'):
        '''
        example:
            key = 'WIND_SPEED__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND'
            elevation = 10
            resolution = '001'
            run_date = '2022-02-13T12:00:00Z'
            pred_date = '2022-02-13T13:00:00Z'
            lats = '(38.566787031305246,57.685939593908294)'
            lons = '(-15.262097813129778,15.413520298424451)'
        '''
        url = ''.join([
            f'https://public-api.meteofrance.fr/public/arome/1.0/',
            f'wcs/MF-NWP-HIGHRES-AROME-{resolution}-FRANCE-WCS/GetCoverage',
            f'?SERVICE=WCS',
            f'&VERSION=2.0.1',
            f'&REQUEST=GetCoverage',
            f'&format=application/wmo-grib',
            f'&coverageId={key}___{run_date}',
            f'&subset=time({pred_date})',
            f'&subset=lat{lats}',
            f'&subset=long{lons}',
        ])
        if elevation:
            url += f'&subset=height({elevation})'
        
        file_name = f"AROME_{resolution}_SP1_{key.split('__')[0]}_{run_date}_{pred_date}.grib2"
        
        return url, file_name

    def download_file(self, url, file_path):
        #print(f'Downloading {file_path}')
        if os.path.isfile(file_path):
            print('Already DL')
            return
        headers = {
            'accept': '*/*',
            'apikey': self.apiKey}
        try:
            with requests.get(url, headers=headers, stream=True) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192): 
                        f.write(chunk)
        except Exception as e:
            print('Failed with exception ', e)

    def download_group(self, offset=0):
        '''Download a set of predictions (wind, temp) for 3 hours prediction'''
        pred_dates = self.get_run_and_pred_dates(offset)
        run_date = pred_dates[0]
        for pred_date in pred_dates:
            for key, elevation, short_key in zip(self.keys, self.elevations, self.short_keys):
                url, file_name = self.get_url_and_fName(
                    key=key,
                    elevation=elevation, 
                    resolution='001',
                    run_date=run_date,
                    pred_date=pred_date
                )
                self.download_file(
                    url, os.path.join(self.dir_path, file_name))

    def split_fname(self, fname):
        splits = fname.split('.')[0].split('_')
        component = '_'.join(splits[3: len(splits) - 2])
        return {
            'model': splits[0],
            'resolution':  splits[1],
            'thingy': splits[2],
            'component': component,
            'run_date': splits[-2],
            'pred_date': splits[-1]
        }

    def compress_group(self):
        files = sorted([f for f in os.listdir('./runs/') if f.endswith('.grib2')])
        grbs = [pygrib.open('./runs/' + f) for f in files]

        # Check if there is enough files
        if len(files) != 12:
            return

        # Check if dates already are in winds file
        dates = list(set(np.datetime64(Downloader().split_fname(f)['pred_date'][:-1])
                         for f in files))
        with h5py.File('winds.h5','a') as h5f:
            if 'dates' in h5f.keys():
                for date in dates:
                    if date in h5f['dates']:
                        return

        print(f'adding {str(dates)}')
        # Open wind file and add runs
        with h5py.File('winds.h5','a') as h5f:
            for file, grb in zip(files, grbs):
                arr = grb.select()[0].values.astype(np.float32)
                arr = np.expand_dims(arr, 0)
                f_info = Downloader().split_fname(file)
                component = f_info['component']
                if component == 'TEMPERATURE':
                    component = 'temp'
                date = np.datetime64(f_info['pred_date'][:-1])

                # Store temperature, U, and V components
                if component in ('temp', 'U_COMPONENT_OF_WIND', 'V_COMPONENT_OF_WIND'):
                    append_component(h5f, component, arr)

                # Store ground heigt only once
                if (f_info['component'] == 'GEOMETRIC_HEIGHT') and ('ground' not in h5f.keys()):
                    h5f.create_dataset(
                        'ground', data=arr, compression="gzip", chunks=True)

                # Store run dates
                if 'dates' not in h5f.keys():
                    date = np.expand_dims(date, 0)
                    date = date.astype(h5py.opaque_dtype(date.dtype))
                    shape = (None, *date.shape[1:])
                    h5f.create_dataset(
                        'dates', data=date, compression="gzip", chunks=True, maxshape=shape)
                elif date not in h5f['dates']:
                    h5f['dates'].resize((h5f['dates'].shape[0] + 1), axis=0)
                    h5f['dates'][-1:] = date.astype(h5py.opaque_dtype(date.dtype))

            # Transform U and V componant to Dir and Speed
            u = h5f['U_COMPONENT_OF_WIND'][:]
            v = h5f['V_COMPONENT_OF_WIND'][:]

            # Add wind direction component
            arr = np.arctan2(v, u)
            component = 'dir'
            append_component(h5f, component, arr)

            # Add wind speed component
            arr = np.sqrt(u**2, v**2)
            component = 'speed'
            append_component(h5f, component, arr)

            # Clean elements of dataset
            del h5f['U_COMPONENT_OF_WIND']
            del h5f['V_COMPONENT_OF_WIND']

    def delete_group(self):
        for files in os.listdir('runs'):
            os.remove('runs/' + files)


Downloader().delete_group()
for i in range(3, 0, -1):
    Downloader().download_group(i)
    Downloader().compress_group()
    Downloader().delete_group()

h5f = h5py.File('winds.h5', 'a')