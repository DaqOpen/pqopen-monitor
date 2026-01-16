# -*- coding: utf-8 -*-
"""
Created on Wed Dec 10 14:29:27 2025

@author: Standard
"""

import requests
import polars as pl
from io import BytesIO
import datetime
import logging
import os

logger = logging.getLogger(__name__)

app_env = os.getenv("DAQPEN_ENV", "development")
if app_env == "development":
    from dotenv import load_dotenv
    load_dotenv()

INFLUXDB_URL = os.getenv("PQOPEN_INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("PQOPEN_INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("PQOPEN_INFLUXDB_ORG", "pqopen")

headers = {
    "Authorization": f"Token {INFLUXDB_TOKEN}",
    "Accept": "application/csv",
    "Content-type": "application/vnd.flux"
}

def read_data_pl(start_dt, stop_dt, location, bucket, measurement = "aggregated-data", channels = [], interval_sec = None):
    # Check max. Lenght of query
    if channels:
        metadata = {'start_time': start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                    'stop_time': stop_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    'loc': location,
                    'channels': '" or r["_field"] == "'.join(channels),
                    'additional_filter': f'|> filter(fn: (r) => r["interval_sec"] == "{interval_sec:d}")\n' if interval_sec else ""
                  }
        
        query = f"""
        from(bucket: "{bucket}")
          |> range(start: {metadata['start_time']:s}, stop: {metadata['stop_time']:s})
          |> filter(fn: (r) => r["_measurement"] == "{measurement}" and r["location_name"] == "{metadata['loc']:s}")
          {metadata['additional_filter']:s}
          |> filter(fn: (r) => r["_field"] == "{metadata['channels']:s}")
          |> keep(columns: ["_time", "_field", "_value"])
        """
    else:
        metadata = {'start_time': start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                    'stop_time': stop_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    'loc': location,
                    'additional_filter': f'|> filter(fn: (r) => r["interval_sec"] == "{interval_sec:d}")\n' if interval_sec else ""}
        
        query = f"""
        from(bucket: "{bucket}")
          |> range(start: {metadata['start_time']:s}, stop: {metadata['stop_time']:s})
          |> filter(fn: (r) => r["_measurement"] == "{measurement}" and r["location_name"] == "{metadata['loc']:s}")
          {metadata['additional_filter']:s}
          |> keep(columns: ["_time", "_field", "_value"])
        """
    response = requests.post(INFLUXDB_URL+"/api/v2/query", params={"org": INFLUXDB_ORG}, headers=headers, data=query)
    # Load Response with Polars CSV Reader
    pl_df = pl.read_csv(BytesIO(response.content))
    try:
        pl_df = pl_df.drop(["", "result", "table"])
        pl_df = pl_df.filter(pl.col("_field").is_not_null())
        pl_df = pl_df.with_columns(pl.col("_time").str.to_datetime(time_zone="UTC"))
        pl_df = pl_df.pivot(index="_time", on="_field", values="_value").sort("_time")
    except:
        return None
    return pl_df

def read_fields(bucket, measurement = "aggregated-data"):
    query = f"""
    import "influxdata/influxdb/schema"

    schema.measurementFieldKeys(
      bucket: "{bucket}",
      measurement: "{measurement}"
    )
    """
    response = requests.post(INFLUXDB_URL+"/api/v2/query", params={"org": INFLUXDB_ORG}, headers=headers, data=query)
    pl_df = pl.read_csv(BytesIO(response.content))
    pl_df = pl_df.drop(["", "result", "table"])
    pl_df = pl_df.filter(pl.col("_value").is_not_null())
    pl_df = pl_df.rename({"_value": "fields"})

    return pl_df.to_dict(as_series=False)

def read_locations(bucket, measurement = "aggregated-data"):
    query = f"""
    import "influxdata/influxdb/schema"

    schema.measurementTagValues(
      bucket: "{bucket}",
      measurement: "{measurement}",
      tag: "location_name"
    )
    """
    response = requests.post(INFLUXDB_URL+"/api/v2/query", params={"org": INFLUXDB_ORG}, headers=headers, data=query)
    pl_df = pl.read_csv(BytesIO(response.content))
    pl_df = pl_df.drop(["", "result", "table"])
    pl_df = pl_df.filter(pl.col("_value").is_not_null())
    pl_df = pl_df.rename({"_value": "locations"})

    return pl_df.to_dict(as_series=False)

def read_agg_intervals(bucket, measurement = "aggregated-data"):
    query = f"""
    import "influxdata/influxdb/schema"

    schema.measurementTagValues(
      bucket: "{bucket}",
      measurement: "{measurement}",
      tag: "interval_sec"
    )
    """
    response = requests.post(INFLUXDB_URL+"/api/v2/query", params={"org": INFLUXDB_ORG}, headers=headers, data=query)
    pl_df = pl.read_csv(BytesIO(response.content))
    pl_df = pl_df.drop(["", "result", "table"])
    pl_df = pl_df.filter(pl.col("_value").is_not_null())
    pl_df = pl_df.rename({"_value": "interval_sec"})

    return pl_df.to_dict(as_series=False)
    
if __name__ == "__main__":
    start_time = datetime.datetime(2025,11,23,0, tzinfo=datetime.UTC)
    stop_time = datetime.datetime(2025,11,24,0, tzinfo=datetime.UTC)

    data = read_data_pl(start_time, stop_time, "DE/Berlin", "test", channels=["P1", "P2", "P3"])
    fields = read_fields( "test", "aggregated-data")
    print(fields)