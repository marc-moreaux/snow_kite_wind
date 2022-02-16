'''
Download wind and temperature data from meteoFrance API
https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=131&id_rubrique=51
'''
import os
from datetime import datetime, timedelta
import requests
import time

apiKey = 'eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJtYXJjLm1vcmVhdXhAY2FyYm9uLnN1cGVyIiwiYXBwbGljYXRpb24iOnsib3duZXIiOiJtYXJjLm1vcmVhdXgiLCJ0aWVyUXVvdGFUeXBlIjpudWxsLCJ0aWVyIjoiVW5saW1pdGVkIiwibmFtZSI6ImtpdGVfd2luZF9maW5kZXIiLCJpZCI6NzAzLCJ1dWlkIjoiNDAzZGJlN2YtMzJhOS00YzdiLTkzNjUtMTIzMzA2NDY1ZTRhIn0sImlzcyI6Imh0dHBzOlwvXC9wb3J0YWlsLWFwaS5tZXRlb2ZyYW5jZS5mcjo0NDNcL29hdXRoMlwvdG9rZW4iLCJ0aWVySW5mbyI6eyI1MFBlck1pbiI6eyJ0aWVyUXVvdGFUeXBlIjoicmVxdWVzdENvdW50IiwiZ3JhcGhRTE1heENvbXBsZXhpdHkiOjAsImdyYXBoUUxNYXhEZXB0aCI6MCwic3RvcE9uUXVvdGFSZWFjaCI6dHJ1ZSwic3Bpa2VBcnJlc3RMaW1pdCI6MCwic3Bpa2VBcnJlc3RVbml0Ijoic2VjIn19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInBlcm1pdHRlZFJlZmVyZXIiOiIiLCJzdWJzY3JpYmVkQVBJcyI6W3sic3Vic2NyaWJlclRlbmFudERvbWFpbiI6ImNhcmJvbi5zdXBlciIsIm5hbWUiOiJBUk9NRSIsImNvbnRleHQiOiJcL3B1YmxpY1wvYXJvbWVcLzEuMCIsInB1Ymxpc2hlciI6ImFkbWluX21mIiwidmVyc2lvbiI6IjEuMCIsInN1YnNjcmlwdGlvblRpZXIiOiI1MFBlck1pbiJ9XSwiZXhwIjoxNzM5NDYyODI4LCJwZXJtaXR0ZWRJUCI6IiIsImlhdCI6MTY0NDg1NDgyOCwianRpIjoiMmM2MGIxNjQtMTQwZC00MmIyLWE5YzMtMjE0YWQ2N2VkODBlIn0=.tv9VeD0Pn89Gx4eAnKxaFZmRYP1e_wwlu63DVPqGJU0Icwbntcl2vMuGZ01fEMwy38LhGbeCNKC3Nf_BHE7zDP3iSIoxjmeUKU4vIeL2FOyy3af5gZlHETqACgupKVIQ2IduW2b7m-VpMbqZXdYXk4L8l_W7Xuyyz-6ilzCeAj5JJfePbR-Vq-aRovRcKryMk7skqpP5Nq54k882aVAl0e4XTPxH8KQEAw8CpxtJVPqUefOcIyErEsf9usW7RzzQCsuODWMobRX5iOO4h9igKvGX1NhWW8Ay96blAQqbKHgOaTeGZ2QjK-DtTQZnpQJ8hI33jgdmQfyATf0NGMmdGw=='

def get_last_run_date(offset=0):
    '''
    The runs are every 3 hours, from 00h to 21h
    last run full on info is the run before (now - 3h * offset)
    '''
    dt = datetime.now()
    dt = datetime(dt.year, dt.month, dt.day, dt.hour // 3 * 3)
    dt -= timedelta(hours=3 * (offset + 1))
    return dt.isoformat() + 'Z'

def get_next_n_dates(date, n_dates=3):
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

def get_url_and_fName(what, elevation, resolution, run_date, pred_date):
    '''
    example:
        what = 'WIND_SPEED__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND'
        elevation = 10
        resolution = '001'
        run_date = '2022-02-13T12:00:00Z'
        pred_date = '2022-02-13T13:00:00Z'
    '''
    url = ''.join([
        f'https://public-api.meteofrance.fr/public/arome/1.0/',
        f'wcs/MF-NWP-HIGHRES-AROME-{resolution}-FRANCE-WCS/GetCoverage',
        f'?SERVICE=WCS',
        f'&VERSION=2.0.1',
        f'&REQUEST=GetCoverage',
        f'&format=application/wmo-grib',
        f'&coverageId={what}___{run_date}',
        f'&subset=time({pred_date})',
        f'&subset=lat(38.566787031305246,57.685939593908294)',
        f'&subset=long(-15.262097813129778,15.413520298424451)',
    ])
    if elevation:
        url += f'&subset=height({elevation})'
    
    file_name = f"AROME_{resolution}_SP1_{what.split('__')[0]}_{run_date}_{pred_date}"
    
    return url, file_name

def download_file(url, file_path):
    print(f'Downloading {file_path}')
    if os.path.isfile(file_path):
        print('Already DL')
        return
    headers = {
        'accept': '*/*',
        'apikey': apiKey}
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)


# Define DL parameters
for i in range(5):
    run_date = get_last_run_date(i)
    whats, elevations = zip(*[
        ('TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND', 2),
        ('V_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND', 10),
        ('U_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND', 10),
        ('GEOMETRIC_HEIGHT__GROUND_OR_WATER_SURFACE', None),
    ])

    for what, elevation in zip(whats, elevations):
        for pred_date in get_next_n_dates(run_date, 2):
            url, file_name = get_url_and_fName(
                what=what,
                elevation=elevation, 
                resolution='001',
                run_date=run_date,
                pred_date=pred_date
            )
            dir_path = os.path.join(os.getcwd(), 'runs2')
            file_path = os.path.join(dir_path, file_name)
            os.makedirs(dir_path, exist_ok=True)
            download_file(url, file_path)