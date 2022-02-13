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


def get_n_dates_from(from_date=datetime.now() - timedelta(days=1), n_files=4):
    from_date += timedelta(hours=from_date.hour // 6 * 6 - from_date.hour)
    dates = [from_date]
    for i in range(n_files):
        new_date = dates[-1] + timedelta(hours=6)
        dates.append(new_date)
    return [d.isoformat() for d in dates]

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


# Retrieve last 4 short-range forecast
run_times = get_n_dates_from()
time_range = '00H06H'

files = []
for run_time in run_times:
    url = create_url(run_time, time_range)
    file_name = create_file_name(run_time, time_range)
    dir_path = os.path.join(os.getcwd(), 'runs')
    file_path = os.path.join(dir_path, file_name)
    os.makedirs(dir_path, exist_ok=True)
    cmd = f'wget -q {url} -O /tmp/grb && mv /tmp/grb {file_path}'
    print(cmd)
    subprocess.call(cmd, shell=True)
