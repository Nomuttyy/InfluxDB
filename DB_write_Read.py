import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from tqdm import tqdm


class DBWriteRead():
    def __init__(self, bucket: str, org: str, token: str, url: str) -> None:
        self.bucket = bucket
        self.org = org
        self.token = token
        self.url=url
        self.client = influxdb_client.InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org
        )

    def write(self, filename: str):
    # フォルダーの中身forでまわす
        dc = pd.read_csv(filename, encoding="utf-8", index_col=0, parse_dates=True)
        for ts in tqdm(dc.index):
            write_api = self.client.write_api(write_options=SYNCHRONOUS)
            point = influxdb_client.Point("test").time(ts + relativedelta(months=4))
            for col in dc.columns:
                point.field(col, float(dc.loc[ts][col]))
            write_api.write(bucket=self.bucket, org=self.org, record=point)
            write_api.close()
    
    def dfwrite(self, filename):
        dc = pd.read_csv(filename, encoding="utf-8", index_col=0, parse_dates=True)
        # for ts in tqdm(dc.index):
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        #     point = influxdb_client.Point("test").time(ts + relativedelta(months=4))
        #     for col in dc.columns:
        #         point.field(col, float(dc.loc[ts][col]))
        write_api.write(bucket=self.bucket, org=self.org, record=dc, data_frame_measurement_name="aaa")
        write_api.close()

if __name__ == "__main__":
    import configparser
    config_ini = configparser.ConfigParser()
    config_ini.read('config.ini')
    bucket = config_ini['DEFAULT']['bucket']
    org = config_ini['DEFAULT']['org']
    token = config_ini['DEFAULT']['token']
    url = config_ini['DEFAULT']['url']
    test = DBWriteRead(bucket, org, token, url)
    test.dfwrite("/home/nomura/Downloads/20201228_000008F0.CSV")







