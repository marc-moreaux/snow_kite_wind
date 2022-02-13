'''
SOURCE: https://www.architecture-performance.fr/ap_blog/fetching-arome-weather-forecasts-and-plotting-temperatures/
Data URL: http://dcpc-nwp.meteo.fr/services/PS_GetCache_DCPCPreviNum\' \
    + f'?token\=' \
    + f'\&model\=AROME' \
    + f'\&grid\=0.025' \
    + f'\&package\=SP1' \
    + f'\&time\=00H06H' \
    + f'\&referencetime\=2020-01-20T06:00:00Z' \
    + f'\&format\=grib2'
'''
import os
from datetime import datetime, timedelta
import subprocess
import numpy as np


def get_n_dates_from(from_date=datetime(2022, 2, 9), n_files=4):
    from_date += timedelta(hours=from_date.hour // 6 * 6 - from_date.hour)
    dates = [from_date]
    for i in range(n_files):
        new_date = dates[-1] + timedelta(hours=6)
        dates.append(new_date)
    return [d.isoformat() for d in dates]

def get_latest_run_time(delay=4):
    ''' Runs are updated at 0am, 3am, 6am, 0pm, 6pm UTC.
        Note that the delay must be adjusted.
    '''
    utc_now = datetime.utcnow()
    candidate = datetime(utc_now.year, utc_now.month, utc_now.day, utc_now.hour) - \
        timedelta(hours=delay)
    run_time = datetime(candidate.year, candidate.month, candidate.day)
    for hour in np.flip(np.sort([3, 6, 12, 18])):
        if candidate.hour >= hour:
            run_time += timedelta(hours=int(hour))
            break
    return run_time.isoformat()

def get_time_range(batch_number=0):
    ''' 7 different 6-hours long time ranges: 0-6H, 7-12H, ... , 37-42H. '''
    assert batch_number in range(7) 
    end = 6 * (batch_number + 1)
    if batch_number == 0:
        start = 0
    else:
        start = end - 5
    time_range = str(start).zfill(2) + 'H' + str(end).zfill(2) +'H'
    return time_range

def create_url(run_time, time_range='00H06H', package='SP1', token='__5yLVTdr-sGeHoPitnFc7TZ6MhBcJxuSsoZp6y0leVHU__'):
    ''' This creates the url string to get the data with 0.025 spatial accuracy.  '''
    assert package in ['HP1', 'HP2', 'HP3', 'IP1', 'IP2', 'IP3', 'IP4', 'IP5', 'SP1', 'SP2', 'SP3']
    url = f'http://dcpc-nwp.meteo.fr/services/PS_GetCache_DCPCPreviNum\?token\={token}' \
    + f'\&model\=AROME\&grid\=0.025\&package\={package}\&time\={time_range}\&referencetime\={run_time}Z\&format\=grib2'
    return url

def create_file_name(run_time, time_range='00H06H', package='SP1'):
    dt = ''.join(run_time.split(':')[0:2]).replace('-', '').replace('T', '')
    file_name = f'AROME_0.025_{package}_{time_range}_{dt}.grib2'
    return file_name


run_time = get_latest_run_time()
run_times = get_n_dates_from(datetime(2022, 2, 11), n_files=10)
time_range = '00H06H'

files = []
for run_time in run_times:
    url = create_url(run_time, time_range)
    file_name = create_file_name(run_time, time_range)
    file_path = os.path.join(os.getcwd(), 'runs', file_name) # files are downloaded locally
    #cmd = f'wget --output-document {file_path} {url}'
    cmd = f'wget -q {url} -O /tmp/grb && mv /tmp/grb {file_path}'
    print(cmd)
    subprocess.call(cmd, shell=True)
