from datetime import datetime, timedelta, timezone, UTC
import logging
import os

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from sqlalchemy import func
from sqlalchemy.orm import Session
from keydatabase import SessionLocal, ApiKey

import polars as pl
from io import BytesIO

import influx2client

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app_env = os.getenv("DAQOPEN_ENV", "development")
mqtt_clean_session = False
if app_env == "development":
    from dotenv import load_dotenv
    mqtt_clean_session = True
    load_dotenv()

INFLUXDB_BUCKET_ST = os.getenv("PQOPEN_INFLUXDB_BUCKET_ST", "short_term")
INFLUXDB_BUCKET_LT = os.getenv("PQOPEN_INFLUXDB_BUCKET_LT", "long_term")
MAX_ELEMENTS = int(os.getenv("PQOPEN_API_MAX_REQUEST_ELEMENTS", 1_000_000))
RATE_LIMIT_PER_HOUR = int(os.getenv("PQOPEN_API_RATE_LIMIT_PER_HOUR", 50))

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class AggDataRequest(BaseModel):
    range_start: datetime = datetime.now(tz=UTC) - timedelta(hours=1)
    range_stop: datetime = datetime.now(tz=UTC)
    location: str
    interval_sec: int = Field(
        default=600, 
        ge=1,     # greater than or equal (>= 1)
        le=600,    # less than or equal (<= 600)
        description="Aggregation-Intervall in Seconds (1-600)."
    )
    fields: list[str] = []

class CbcDataRequest(BaseModel):
    """
    Data Model for Cycle-by-Cycle data request
    """
    range_start: datetime = datetime.now(tz=UTC) - timedelta(hours=1)
    range_stop: datetime = datetime.now(tz=UTC)
    location: str
    fields: list[str] = []


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

app = FastAPI()

def get_api_key(api_key: str = Depends(api_key_header), 
                db: Session = Depends(get_db)):
    """
    Check API Key and return api key object or error
    """
    if api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API-Key in Header 'X-API-Key'."
        )
        
    # Search Database for Key
    key_record = db.query(ApiKey).filter(
        ApiKey.key_hash == api_key,
        ApiKey.is_active == True 
    ).first()

    if key_record:
        seconds_since_limit_reset = (datetime.now(timezone.utc).replace(tzinfo=None) - key_record.rate_limit_reseted_at).total_seconds()
        if key_record.rate_limit_counter < RATE_LIMIT_PER_HOUR:
            # Success, write last_used entry in database
            key_record.last_used = func.now()
            key_record.rate_limit_counter += 1
            db.commit()
        elif seconds_since_limit_reset > 3600:
            key_record.last_used = func.now()
            key_record.rate_limit_counter = 0
            key_record.rate_limit_reseted_at = func.now()
            db.commit()
        else:
            # Rate Limit Exceeded
            raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded for Api-Key. Will be reseted at {(key_record.rate_limit_reseted_at+timedelta(hours=1)).strftime('%H:%M')} UTC"
        )    
        return key_record


    else:
        # Invalid or inactive Key
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or inactive API-Key."
        )
    
def parquet_stream_generator(df: pl.DataFrame):
    """
    Generate in-memory stream object for parquet file
    """
    parquet_buffer = BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    yield parquet_buffer.read()

@app.get("/v1/meta/locations/{family}")
def read_locations(family: str, auth_data: ApiKey = Depends(get_api_key)):
    if family == "cbc":
        locations = influx2client.read_locations(bucket=auth_data.allowed_bucket_st,
                                                 measurement="cycle-by-cycle")
    elif family == "aggregated":
        locations = influx2client.read_locations(bucket=auth_data.allowed_bucket_lt,
                                                 measurement="aggregated-data")
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Data family of ['cbc', 'aggregated'] allowed. Requested: {family}."
        )
    return locations

@app.get("/v1/meta/fields/{family}")
def read_fields(family: str, auth_data: ApiKey = Depends(get_api_key)):
    if family == "cbc":
        fields = influx2client.read_fields(auth_data.allowed_bucket_st, measurement="cycle-by-cycle")
    elif family == "aggregated":
        fields = influx2client.read_fields(auth_data.allowed_bucket_lt, measurement="aggregated-data")
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Data family of ['cbc', 'aggregated'] allowed. Requested: {family}."
        )
    return fields

@app.get("/v1/meta/aggintervals")
def read_intervals(auth_data: ApiKey = Depends(get_api_key)):
    agg_intervals = influx2client.read_agg_intervals(auth_data.allowed_bucket_lt, measurement="aggregated-data")
    return agg_intervals

# Read aggregated data with fixed aggregation interval
@app.post("/v1/data/aggregated")
def read_aggregated_data(data_request: AggDataRequest, auth_data: ApiKey = Depends(get_api_key)):
    duration = data_request.range_stop - data_request.range_start
    num_fields = len(data_request.fields) if data_request.fields else len(read_fields(family="aggregated", auth_data=auth_data)["fields"])
    num_elements = (duration.total_seconds() * num_fields) / data_request.interval_sec
    if num_elements > MAX_ELEMENTS:
        # Errror on max number of elements are exceeded (HTTP 400 Bad Request)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The data range is too big. Maximum 1 Mio elements allowed ({MAX_ELEMENTS:d}). Requested: {num_elements}."
        )
    if num_elements < 0:
        # Check negative time range
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Requested a negative number of elements"
        )
    df = influx2client.read_data_pl(start_dt=data_request.range_start,
                                      stop_dt=data_request.range_stop,
                                      location=data_request.location,
                                      bucket=auth_data.allowed_bucket_lt,
                                      measurement="aggregated-data",
                                      channels=data_request.fields,
                                      interval_sec=data_request.interval_sec)
    parquet_stream = parquet_stream_generator(df)
    return StreamingResponse(
        parquet_stream,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename=agg_{data_request.range_start.strftime('%Y%m%dT%H%M%S')}_{data_request.range_stop.strftime('%Y%m%dT%H%M%S')}.parquet"
        }
    )

# Read cycle-by-cycle raw data
@app.post("/v1/data/cbc")
def read_cbc_data(data_request: CbcDataRequest, auth_data: ApiKey = Depends(get_api_key)):
    duration = data_request.range_stop - data_request.range_start
    num_fields = len(data_request.fields) if data_request.fields else len(read_fields(family="cbc", auth_data=auth_data)["fields"])
    num_elements = (duration.total_seconds() * 50 * num_fields)
    if num_elements > MAX_ELEMENTS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The data range is too big. Maximum 1 Mio elements allowed ({MAX_ELEMENTS:d}). Requested: {num_elements}."
        )
    if num_elements < 0:
        # Check negative time range
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Requested a negative number of elements"
        )
    # Read data from influxdb
    df = influx2client.read_data_pl(start_dt=data_request.range_start,
                                      stop_dt=data_request.range_stop,
                                      location=data_request.location,
                                      bucket=auth_data.allowed_bucket_st,
                                      measurement="cycle-by-cycle",
                                      channels=data_request.fields)
    # Create parquet stream object
    parquet_stream = parquet_stream_generator(df)
    # Return Stream Object
    return StreamingResponse(
        parquet_stream,
        media_type="application/octet-stream", # Standard für Binärdateien
        headers={
            "Content-Disposition": f"attachment; filename=cbc_{data_request.range_start.strftime('%Y%m%dT%H%M%S')}_{data_request.range_stop.strftime('%Y%m%dT%H%M%S')}.parquet"
        }
    )

