
import asyncio
import aiomqtt
import ssl
import orjson
import gzip
import logging
import os
import signal
import cbor2
import math
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api import WriteApi

from dataconverter import convert_dataseries_to_df, cbc_dict_to_line_protocol, agg_dict_to_line_protocol

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app_env = os.getenv("DAQOPEN_ENV", "development")
mqtt_clean_session = False
if app_env == "development":
    from dotenv import load_dotenv
    mqtt_clean_session = True
    load_dotenv()

MQTT_HOST = os.getenv("PQOPEN_MQTT_HOST", "mqtt.pqopen.com")
MQTT_PORT = int(os.getenv("PQOPEN_MQTT_PORT", 8883))
MQTT_USERNAME = os.getenv("PQOPEN_MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("PQOPEN_MQTT_PASSWORD")
MQTT_USE_TLS = True if os.getenv("PQOPEN_MQTT_USE_TLS", "True") == "True" else False
MQTT_TOPIC = os.getenv("PQOPEN_MQTT_TOPIC", "private/#")
MQTT_CLIENT_ID = os.getenv("PQOPEN_MQTT_CLIENT_ID", "mqtt-to-database")
INFLUXDB_URL = os.getenv("PQOPEN_INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("PQOPEN_INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("PQOPEN_INFLUXDB_ORG", "pqopen")

location_cache = {}

def decode_payload(payload: bytes, encoding: str):
    if encoding == "gjson":
        payload_dict = orjson.loads(gzip.decompress(payload))
    elif encoding == "json":
        payload_dict = orjson.loads(payload)
    elif encoding == "cbor":
        payload_dict = cbor2.loads(payload)

    return payload_dict

async def mqtt_listener(write_api: WriteApi, device_config: dict, stop_event: asyncio.Event):
    async def write_dataseries(device_config, data):
        tags = {"location_name": device_config["location_name"],
                "location_lat": device_config["location_lat"],
                "location_lon": device_config["location_lon"]}
        lp_data = cbc_dict_to_line_protocol(m_data=data, tags=tags)
        await write_api.write(bucket=device_config.get("db_dataseries_bucket", "short_term"), 
                              record=lp_data)
        
    async def write_aggdata(device_config, data):
        tags = {"interval_sec": data["interval_sec"],
                "location_name": device_config["location_name"],
                "location_lat": device_config["location_lat"],
                "location_lon": device_config["location_lon"]}
        
        lp_data = agg_dict_to_line_protocol(m_data=data, tags=tags)
        await write_api.write(bucket=device_config.get("db_aggregated_bucket", "long_term"),
                              record=lp_data)
    
    async def write_eventdata(device_config, data):
        json_body = {'measurement': 'event-data',
                     'tags': {'event_type': data["event_type"],
                              'channel': data["channel"],
                              'location_name': device_config["location_name"],
                              'location_lat': device_config["location_lat"],
                              'location_lon': device_config["location_lon"]},
                     'time': int(data['timestamp']*1e9),
                     'fields': data["data"]}
        await write_api.write(bucket=device_config.get("db_event_bucket", "long_term"),
                              record=json_body)

    # Create TLS Kontext
    if MQTT_USE_TLS:
        tls_context = ssl.create_default_context()
        tls_insecure = False
        logger.debug("Use TLS")
    else:
        tls_context = None
        tls_insecure = None
        logger.debug("Don't use TLS")
    async with aiomqtt.Client(MQTT_HOST, port=MQTT_PORT, username=MQTT_USERNAME, password=MQTT_PASSWORD, tls_insecure=tls_insecure, tls_context=tls_context, identifier=MQTT_CLIENT_ID, clean_session=mqtt_clean_session) as client:
        await client.subscribe(MQTT_TOPIC, 2)
        topic_prefix = MQTT_TOPIC.split("/#")[0]
        topic_prefix_num_parts = len(topic_prefix.split("/"))
        async for message in client.messages:
            if stop_event.is_set():
                break
            try:
                parts = message.topic.value.split("/") # {topic-prefix}/{device-id}/{data-type}/{encoding}
                if len(parts) < (topic_prefix_num_parts + 3):
                    logger.warning(f"Unerwartetes Topic-Format: {message.topic}")
                    continue
                device_id, data_type, encoding = parts[-3], parts[-2], parts[-1]
                if device_id in device_config:
                    data = decode_payload(message.payload, encoding)
                    # Multi Part (Bulk) Message = data is of type list and holds multiple messages
                    if isinstance(data, list):
                        for data_packet in data:
                            subtopic_parts = data_packet["subtopic"].split("/")
                            data_type = subtopic_parts[-2]
                            encoding = subtopic_parts[-1]
                            data_snippet = decode_payload(data_packet["payload"], encoding)
                            if data_type == "dataseries":
                                await write_dataseries(device_config=device_config[device_id], data=data_snippet["data"])
                                logger.debug("data Sent")
                            elif data_type == "agg_data":
                                await write_aggdata(device_config=device_config[device_id], data=data_snippet)
                            elif data_type == "event":
                                await write_eventdata(device_config=device_config[device_id], data=data_snippet)
                    # Single Part Messages
                    # Dataseries Message
                    elif data_type == "dataseries":
                        await write_dataseries(device_config=device_config[device_id], data=data["data"])
                        logger.debug("Dataseries written to DB")
                    # Aggregated Data Message
                    elif data_type == "agg_data":
                        await write_aggdata(device_config=device_config[device_id], data=data)
                        logger.debug("Agg-Data written to DB")
                    # Event Data Message
                    elif data_type == "event":
                        await write_eventdata(device_config=device_config[device_id], data=data)
                        logger.debug("Event Data written to DB")
                    else:
                        logger.warning(f"Datatype {data_type} not implemented")
                else:
                    logger.warning(f"Device {device_id} not configured")
            except Exception as e:
                logger.error(f"Fehler bei Nachricht: {str(e)}")


async def main():
    stop_event = asyncio.Event()

    def shutdown():
        logger.info("Shutdown signal received.")
        stop_event.set()
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    # DB-Connection
    db_client = InfluxDBClientAsync(url=INFLUXDB_URL, 
                                    token=INFLUXDB_TOKEN, 
                                    org=INFLUXDB_ORG)
    write_api = db_client.write_api()
    # Load Device Config
    with open("config/device_config.json") as f:
        device_config = orjson.loads(f.read())

    try:
        await mqtt_listener(write_api, device_config, stop_event)
    except Exception as e:
        print("MQTT-Error:", e)
    finally:
        await db_client.__aexit__(None, None, None)
        logger.info("Client closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application stopped by user")
