import configparser

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from tqdm import tqdm


config_ini = configparser.ConfigParser()
config_ini.read('config.ini')
bucket = config_ini['DEFAULT']['bucket']
org = config_ini['DEFAULT']['org']
token = config_ini['DEFAULT']['token']
url = config_ini['DEFAULT']['url']

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)




dc = pd.read_csv('/home/nomura/Downloads/20210511_000000BD.CSV',encoding="utf-8",index_col=0,parse_dates=True)
# dc = pd.read_csv('/home/nomura/Downloads/20210511_000000BD.CSV',encoding="utf-8")
columns = dc.columns
# a = influxdb_client.Point("test").time(dc['TIME'])

for ts in tqdm(dc.index):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    point = influxdb_client.Point("test").time(ts + relativedelta(months=4))
    for col in dc.columns:
        point.field(col, float(dc.loc[ts][col]))
    write_api.write(bucket=bucket, org=org, record=point)
    write_api.close()

# for data in columns :
#     try:
#         a.field(data, dc[data])
#     except ValueError :
#         pass

# p = influxdb_client.Point("my_measurement").tag("location", "Prague").field("temperature", 25.3).time()
# write_api.write(bucket=bucket, org=org, record=a)
