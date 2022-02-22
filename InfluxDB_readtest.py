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


query_api = client.query_api()
query = """
    from(bucket:"test") \
        |> range(start: -1y) \
        |> filter(fn:(r) => r._measurement == "test") \
        |> filter(fn:(r) => r._field == "太陽光EZAグリッド電力(W)" )
"""

result = query_api.query(org=org, query=query)
results = []
for table in result:
    for record in table.records:
        results.append((record.get_field(), record.get_value()))

print(results)
