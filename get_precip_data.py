import pandas as pd
import requests
import io
from config import dir_path

'''
Environment Canada provides Google sheet listing stations with station name, station id, data availability start and end year and month. 

https://docs.google.com/spreadsheets/d/1CQDjEv3Hf6FRzztKlVUSGtdCnJ0IxvYZMrA35EmKfAk/edit#gid=681791018
'''

station_name = 'VIDETTE LAKES HARPE LAKE'
station_id = 1217
first_year = 1987
start_date = '1987-06-07'
last_year = 2009
end_date = '2009-05-30'
time_frame = 2
if time_frame == 1: time_frame_word = 'hourly'
else: time_frame_word = 'daily'
filename = 'precip' + '-' + station_name +'-'+ str(station_id) +'-'+ str(time_frame_word) +'-'+ str(first_year) + 'to' + str(last_year) + '.csv'

url_template = 'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={0}&Year={1}&Month={2}&Day=1&timeframe={3}&submit=Download+Data'

cols = ["Longitude (x)","Latitude (y)","Station Name","Climate ID","Date/Time","Year","Month","Day","Data Quality","Max Temp (\xc2\xb0C)","Max Temp Flag","Min Temp (\xc2\xb0C)","Min Temp Flag","Mean Temp (\xc2\xb0C)","Mean Temp Flag","Heat Deg Days (\xc2\xb0C)","Heat Deg Days Flag","Cool Deg Days (\xc2\xb0C)","Cool Deg Days Flag","Total Rain (mm)","Total Rain Flag","Total Snow (cm)","Total Snow Flag","Total Precip (mm)","Total Precip Flag","Snow on Grnd (cm)","Snow on Grnd Flag","Dir of Max Gust (10s deg)","Dir of Max Gust Flag","Spd of Max Gust (km/h)","Spd of Max Gust Flag"]

# empty list for responses
response_list = []

# loop years and months, request, csv like response, append to list
for year in range(int(first_year), int(last_year + 1)):
    for month in range(1, 13):
        url = url_template.format(station_id, year, month, time_frame)
        response = requests.get(url)
        response_csv = io.StringIO(response.content.decode('utf-8'))
        df = pd.read_csv(response_csv, index_col=None, header=0)
        response_list.append(df)
    # print year as measure of progress
    print(year)

# convert list to dataframe
df_precip_raw = pd.concat(response_list, axis=0, ignore_index=True)

# replace forward slash in date/time col name
df_precip_raw.columns=df_precip_raw.columns.str.replace('/','')

# rename columns
df_precip_raw.rename(columns = {'DateTime':'YYYYMMDD','Year':'YYYY','Month':'MM','Day':'DD'},inplace = True)

# make yyyymmdd datetime dtype
df_precip_raw['YYYYMMDD'] = pd.to_datetime(df_precip_raw['YYYYMMDD'])

# filter by start and end dates
df_precip_raw = df_precip_raw[df_precip_raw['YYYYMMDD'].isin(pd.date_range(start_date, end_date))]

# make yyyymmdd string
df_precip_raw['YYYYMMDD'] = df_precip_raw['YYYYMMDD'].dt.strftime('%Y%m%d')

# keep only these columns
df_precip = df_precip_raw[['YYYYMMDD','YYYY','MM','DD','Total Rain (mm)','Total Precip (mm)','Snow on Grnd (cm)','Total Snow (cm)']]

# drop duplicate rows (there are 12 rows per day)
df_precip = df_precip.drop_duplicates(subset=['YYYYMMDD','YYYY','MM','DD','Total Rain (mm)','Total Precip (mm)','Snow on Grnd (cm)','Total Snow (cm)'], keep='first')

## export dataframe to csv
df_precip.to_csv(dir_path + filename, index=False, header=True, encoding='utf-8')
