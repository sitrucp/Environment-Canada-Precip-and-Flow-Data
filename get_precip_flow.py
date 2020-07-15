'''
Data came from https://kwt.bcwatertool.ca/streamflow
For station Deadman River Above Criss Creek 08LF027
From April 2013 to Jul 2020
Download button click downloads file station-data-3819.csv
station-data-3819.csv has 4 columns:
* Analysis = Discharge (m3/s) (not used)
* Datetime = mm/dd/yyyy (reformatted to yyyymmdd)
* Value = float 2 decimals (used as is)
* QA = 1 or 0 (not used)

precip data file from get_precip.py csv output
'''
import pandas as pd
from config import dir_path

# create path and file variables
flow_file = 'flow-station-data-3819.csv'
precip_file = 'precip-70MILE-1321-daily-1974to1987.csv'
precip_flow_file = 'flow and precip-70 mile precip.csv'

## Data starts at row 16, so drop first 15 rows
df_flow = pd.read_csv(dir_path + flow_file, skiprows=15)
## Analysis col has 'Water Level (m)' & 'Discharge (m3/s)' 
## filter to whichever is wanted
df_flow.loc[df_flow['Analysis'] == 'Discharge (m3/s)']
## drop unwanted cols
df_flow.drop(['Analysis','QA'], axis=1, inplace=True)
## rename cols
df_flow.rename(columns = {'Datetime':'YYYYMMDD','Value':'Flow'},inplace = True)
## change to datetime format to change to desired format
df_flow['YYYYMMDD'] = pd.to_datetime(df_flow['YYYYMMDD'])
df_flow['YYYYMMDD'] = df_flow['YYYYMMDD'].dt.strftime('%Y%m%d')
## create int value YYYY, MM, DD
df_flow['YYYY'] = df_flow['YYYYMMDD'].astype(str).str[:4].astype(int)
df_flow['MM'] = df_flow['YYYYMMDD'].astype(str).str[4:6].astype(int)
df_flow['DD'] = df_flow['YYYYMMDD'].astype(str).str[-2:].astype(int)
df_flow['YYYYMMDD'] = df_flow['YYYYMMDD'].astype(str).astype(int)
## filter to YYYY's wanted
#df_flow = df_flow.loc[(df_flow['YYYY'].astype(int) >= )]

# get precip data into dataframe
df_precip = pd.read_csv(dir_path + precip_file)
## exclude YYYY, MM, DD from precip as they are already in df flow
df_precip = df_precip[['YYYYMMDD','Total Rain (mm)','Total Precip (mm)','Snow on Grnd (cm)','Total Snow (cm)']]

# join dataframes on YYYYMMDD values
df_flow_precip = pd.merge(
    df_flow, 
    df_precip, 
    left_on='YYYYMMDD', 
    right_on='YYYYMMDD', 
    how='left',
    indicator=True)
# indicator shows both, left only, can filter on this in report

# add col for precip file name
df_flow_precip['precip_file'] = precip_file

# filter to only desired columns
df_flow_precip = df_flow_precip[['precip_file','YYYYMMDD','YYYY','MM','DD','Flow','Total Rain (mm)','Total Precip (mm)','Snow on Grnd (cm)','Total Snow (cm)','_merge']]

df_flow_precip.to_csv(dir_path + precip_flow_file, index=False, header=True, encoding='utf-8')

