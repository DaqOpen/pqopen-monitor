import signal
import logging
import datetime
import time
import json
import re
import os
from pathlib import Path
import influx2client

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, signum, frame):
    # Actually Stop the Event Loop
    self.kill_now = True

def clean_string(string_to_clean: str):
    res = re.sub(r'[^a-zA-Z0-9_]', '_', string_to_clean)
    return res

app_env = os.getenv("DAQPEN_ENV", "development")
if app_env == "development":
    from dotenv import load_dotenv
    load_dotenv()

INFLUXDB_BUCKET_ST = os.getenv("PQOPEN_INFLUXDB_BUCKET_ST", "short_term")
INFLUXDB_BUCKET_LT = os.getenv("PQOPEN_INFLUXDB_BUCKET_LT", "long_term")

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

with open("archiver_config.json") as f:
    app_config = json.load(f)

app_killer = GracefulKiller()

scheduler_next_run_ts = datetime.datetime.now(tz=datetime.UTC)
# Set scheduler start to 01:00 of today (UTC-Time)
scheduler_next_run_ts = scheduler_next_run_ts.replace(hour=1, minute=0, second=0, microsecond=0)

while not app_killer.kill_now:
    # Wait for next scheduler time slot
    while datetime.datetime.now(tz=datetime.UTC) < scheduler_next_run_ts:
        time.sleep(10)
    data_range_end = scheduler_next_run_ts.replace(hour=0)
    data_range_start = data_range_end - datetime.timedelta(days=1)
    print(data_range_start, data_range_end)
    for archiver_task in app_config["tasks_daily"]:
       # Read Data from Database
       pl_df = influx2client.read_data_pl(
          start_dt=data_range_start,
          stop_dt=data_range_end,
          location=archiver_task["location_name"],
          bucket=INFLUXDB_BUCKET_ST,          
          measurement=archiver_task["measurement"],
          channels=archiver_task["channels"])
       if pl_df is None:
          logger.error("No Data! " + str(archiver_task))
          continue
       file_path = Path(app_config["output_path"] + "/daily/" + clean_string(archiver_task["location_name"]))
       file_path.mkdir(parents=True, exist_ok=True)
       pl_df.write_parquet((file_path/(data_range_start.strftime("%Y-%m-%d_")+",".join(archiver_task["channels"])+".parquet")).as_posix())
       logger.info("Task completed and file written to " + file_path.as_posix())
    
    scheduler_next_run_ts += datetime.timedelta(days=1)
    
