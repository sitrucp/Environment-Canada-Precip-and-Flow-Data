import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
from config import dir_path, sqlite_db_path

'''
Download 'Hydrat' National Water Data Archive sqlite db from here:

https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html

Has all Canada stations flow and level to current date minus one month roughly
'''
## get db file
sqlite_db_file = sqlite_db_path + '/Hydat.sqlite3'

## hydrometric station id
station_id = '08LF027'

## output csv filename
output_filename = 'flow-' + station_id + '.csv'

## create sql query
sql_query = """select 
STATION_NUMBER,
YEAR,
MONTH,
MONTHLY_TOTAL,
FLOW1,
FLOW10,
FLOW11,
FLOW12,
FLOW13,
FLOW14,
FLOW15,
FLOW16,
FLOW17,
FLOW18,
FLOW19,
FLOW2,
FLOW20,
FLOW21,
FLOW22,
FLOW23,
FLOW24,
FLOW25,
FLOW26,
FLOW27,
FLOW28,
FLOW29,
FLOW3,
FLOW30,
FLOW31,
FLOW4,
FLOW5,
FLOW6,
FLOW7,
FLOW8,
FLOW9
FROM DLY_FLOWS 
where station_number = "{station_id}" 
and YEAR > 1981;""".format(station_id=station_id)

## create sqlite db connection
conn = sqlite3.connect(sqlite_db_file)

## execute sql query and get results Pandas read_sql_query
df_flow_raw = pd.read_sql_query(sql_query, conn)
conn.close()

## create new column YYYY-MM
df_flow_raw['YYYYMM'] = df_flow_raw.YEAR.astype(str) + df_flow_raw.MONTH.map('{:02}'.format).astype(str)

## re-shape df from wide to long (unpivot flowxx cols to rows)
df_flow = df_flow_raw.melt(id_vars = ['STATION_NUMBER','YEAR','MONTH','YYYYMM','MONTHLY_TOTAL'], var_name = 'FLOWDAY', value_name = 'DAILY_TOTAL')

## create DD from date
df_flow['DAY'] = df_flow['FLOWDAY'].str.extract('(\d+)').astype(int)

## create new column YYYY-MM-DD
df_flow['YYYYMMDD'] = df_flow['YYYYMM'].astype(str) + df_flow['DAY'].map('{:02}'.format).astype(str)

## drop unneed col
df_flow.drop(['FLOWDAY'], axis=1, inplace=True)

## sort results by date
df_flow.sort_values('YYYYMMDD')

## export dataframe to csv
df_flow.to_csv(dir_path + output_filename, index=False, header=True, encoding='utf-8')
