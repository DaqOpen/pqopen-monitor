# influx_client.py

from influxdb_client import InfluxDBClient
import os
import logging
import datetime

logger = logging.getLogger(__name__)

app_env = os.getenv("DAQPEN_ENV", "development")
if app_env == "development":
    from dotenv import load_dotenv
    load_dotenv()

INFLUXDB_URL = os.getenv("PQOPEN_INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("PQOPEN_INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("PQOPEN_INFLUXDB_ORG", "pqopen")
INFLUXDB_BUCKET_ST = os.getenv("PQOPEN_INFLUXDB_BUCKET_ST", "short_term")
INFLUXDB_BUCKET_LT = os.getenv("PQOPEN_INFLUXDB_BUCKET_LT", "long_term")

client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG,
    timeout=60000
)

query_api = client.query_api()

def query_data_range(measurement: str, location: str, start_dt: datetime.datetime, end_dt: datetime.datetime, fields: list[str], bucket: str = INFLUXDB_BUCKET_ST):
    field_filter = ' or '.join([f'r["_field"] == "{f}"' for f in fields])
    query = f'''
    from(bucket: "{bucket}")
      |> range(start: {start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}, stop: {end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")})
      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
      |> filter(fn: (r) => r["location_name"] == "{location}")
      |> filter(fn: (r) => {field_filter})
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
      |> drop(columns: ["_start", "_stop", "_measurement", "location_name", "location_lat", "location_lon"])
    '''
    print(query)

    data_frame = query_api.query_data_frame(query, data_frame_index=["_time"])
    if data_frame.empty:
        logger.info(query)
        return None
    data_frame.drop(columns=["result", "table"], inplace=True)
    return data_frame
