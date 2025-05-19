
import asyncio
import aiomqtt
import ssl
import json
import gzip
import logging
import os
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api import WriteApi

from dataconverter import convert_dataseries_to_df

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app_env = os.getenv("DAQPEN_ENV", "development")
if app_env == "development":
    from dotenv import load_dotenv
    load_dotenv()

MQTT_HOST = os.getenv("PQOPEN_MQTT_HOST", "mqtt.pqopen.com")
MQTT_PORT = int(os.getenv("PQOPEN_MQTT_PORT", 8883))
MQTT_USERNAME = os.getenv("PQOPEN_MQTT_USERNAME", "testuser")
MQTT_PASSWORD = os.getenv("PQOPEN_MQTT_PASSWORD", "testpass")
MQTT_TOPIC = os.getenv("PQOPEN_MQTT_TOPIC", "private/#")
INFLUXDB_URL = os.getenv("PQOPEN_INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("PQOPEN_INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("PQOPEN_INFLUXDB_ORG", "pqopen")

location_cache = {}

def decode_payload(payload: bytes, encoding: str):
    if encoding == "gjson":
        payload_dict = json.loads(gzip.decompress(payload))
    elif encoding == "json":
        payload_dict = json.loads(payload)

    return payload_dict

async def mqtt_listener(write_api: WriteApi, device_config: dict):
    """Async: empfängt MQTT-Nachrichten und legt sie in die Queue."""
    tls_context = ssl.create_default_context()
    async with aiomqtt.Client(MQTT_HOST, port=MQTT_PORT, username=MQTT_USERNAME, password=MQTT_PASSWORD, tls_insecure=False, tls_context=tls_context) as client:
        await client.subscribe(MQTT_TOPIC)
        async for message in client.messages:
            try:
                parts = message.topic.value.split("/")
                if len(parts) < 4:
                    logger.warning(f"Unerwartetes Topic-Format: {message.topic}")
                    continue
                device_id, data_type, encoding = parts[1], parts[2], parts[3]
                if device_id in device_config:
                    data = decode_payload(message.payload, encoding)
                    if data_type == "dataseries":
                        df = convert_dataseries_to_df(data["data"])
                        df.loc[:,"location_name"] = device_config[device_id]["location_name"]
                        df.loc[:,"location_lat"] = device_config[device_id]["location_lat"]
                        df.loc[:,"location_lon"] = device_config[device_id]["location_lon"]
                        del df["timestamp"]
                        await write_api.write(bucket="short_term", 
                                            record=df,
                                            data_frame_measurement_name='cycle-by-cycle',
                                            data_frame_tag_columns=['location_name', "location_lat", "location_lon"])
                        print("data Sent")
                else:
                    print("Device not configured")
            except Exception as e:
                print("Fehler bei Nachricht:", e)


async def main():
    # DB-Verbindung
    db_client = InfluxDBClientAsync(url=INFLUXDB_URL, 
                                    token=INFLUXDB_TOKEN, 
                                    org=INFLUXDB_ORG)
    write_api = db_client.write_api()
    with open("config/device_config.json") as f:
        device_config = json.load(f)
    try:
        await mqtt_listener(write_api, device_config)
    except Exception as e:
        print("MQTT-Fehler:", e)

if __name__ == "__main__":
    asyncio.run(main())