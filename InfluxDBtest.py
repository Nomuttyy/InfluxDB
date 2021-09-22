import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

bucket = "test"
org = "saitolab"
token = "cOEA1hCOJE6G1_JAej9HEuW5u0t9ESXFQBGjhATn23UcpVi49A6jHqPwx0p_GZKKGLOT2LYfXKEKkyO1aUwGAw=="
# Store the URL of your InfluxDB instance
url="http://202.13.164.62:8086"

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
