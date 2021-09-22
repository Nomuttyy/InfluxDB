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
