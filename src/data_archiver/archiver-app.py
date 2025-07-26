import signal
import logging
import datetime
import time
import json
import pyarrow as pa
import pyarrow.parquet as pq
import re
from pathlib import Path
from influx_client import query_data_range

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
       df = query_data_range(measurement=archiver_task["measurement"],
                             location=archiver_task["location_name"],
                             start_dt=data_range_start,
                             end_dt=data_range_end,
                             fields=archiver_task["channels"])
       if df is None:
          logger.error("No Data! " + str(archiver_task))
          continue
       table = pa.Table.from_pandas(df)
       file_path = Path(app_config["output_path"] + "/daily/" + clean_string(archiver_task["location_name"]))
       file_path.mkdir(parents=True, exist_ok=True)
       pq.write_table(table, (file_path/(data_range_end.strftime("%Y-%m-%d_")+",".join(df.columns)+".parquet")).as_posix())
       logger.info("Task completed and file written to " + file_path.as_posix())
    
    scheduler_next_run_ts += datetime.timedelta(days=1)
    
