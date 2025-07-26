import signal
import logging
import os
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
import datetime
import time
from scipy.signal import welch
import pandas as pd
import numpy as np
from pqopen.helper import floor_timestamp

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, signum, frame):
    # Actually Stop the Event Loop
    self.kill_now = True

def query_cycle_by_cycle_data(query_api: QueryApi, start_time: datetime.datetime, stop_time: datetime.datetime, location_name: str, channels: list):
    metadata = {'start_time': start_time.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                'stop_time': stop_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                'location_name': location_name,
                'channels': '" or r["_field"] == "'.join(channels)}
    
    query = f"""
    from(bucket: "short_term")
      |> range(start: {metadata['start_time']:s}, stop: {metadata['stop_time']:s})
      |> filter(fn: (r) => r["_measurement"] == "cycle-by-cycle" and r["location_name"] == "{metadata['location_name']:s}")
      |> filter(fn: (r) => r["_field"] == "{metadata['channels']:s}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
      |> drop(columns: ["_start", "_stop", "_measurement", "location_name", "location_lat", "location_lon"])
    """
    
    fill_query = "\n".join([f'|> fill(column: "{channel:s}", usePrevious: true)' for channel in channels])
    query += fill_query
    data_frame = query_api.query_data_frame(query, data_frame_index=["_time"])
    if data_frame.empty:
        logger.info(query)
        return None
    data_frame.drop(columns=["result", "table"], inplace=True)
    return data_frame

def process_freq_psd_spectrum(query_api, writer_api, start_time: datetime.datetime, stop_time: datetime.datetime, location_name: str):
    fs = 50 # Set Samplerate in Hz
    # Gather Frequency Data from Database
    data_frame = query_cycle_by_cycle_data(query_api, start_time, stop_time, location_name, ["Freq"])
    if data_frame is None:
       return None
    frequencies, psd = welch(data_frame["Freq"].values, nperseg=5000,fs=fs) # 10 mHz Resolution
    psd_df = pd.DataFrame(data=[20*np.log10(psd[:300])], columns=[f"{freq:.3f} Hz" for freq in frequencies[:300]], index=[stop_time])
    psd_df.loc[:, "location_name"] = location_name
    writer_api.write("calculated_data", 
                     record=psd_df, 
                     data_frame_measurement_name="psd", 
                     data_frame_tag_columns=["location_name"])   


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app_env = os.getenv("DAQPEN_ENV", "development")
if app_env == "development":
    from dotenv import load_dotenv
    load_dotenv()

INFLUXDB_URL = os.getenv("PQOPEN_INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("PQOPEN_INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("PQOPEN_INFLUXDB_ORG", "pqopen")

app_killer = GracefulKiller()

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()
writer_api = client.write_api(write_options=SYNCHRONOUS)

next_round_ts = floor_timestamp(time.time(), 900, "s") + 900

while not app_killer.kill_now:
    while time.time() < next_round_ts:
        time.sleep(1)
    calc_dt_start = datetime.datetime.fromtimestamp(next_round_ts - 900, tz=datetime.UTC)
    calc_dt_end = datetime.datetime.fromtimestamp(next_round_ts, tz=datetime.UTC)
    print(calc_dt_start, calc_dt_end)
    process_freq_psd_spectrum(query_api, writer_api, calc_dt_start, calc_dt_end, "Graz")
    process_freq_psd_spectrum(query_api, writer_api, calc_dt_start, calc_dt_end, "DE/Berlin")
    process_freq_psd_spectrum(query_api, writer_api, calc_dt_start, calc_dt_end, "CH/Solothurn")
    next_round_ts += 900
    
