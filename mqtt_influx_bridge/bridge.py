import json
import logging
import math
import os
import re
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "#")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt-influx-bridge")
MQTT_TRANSPORT = os.getenv("MQTT_TRANSPORT", "tcp").lower()
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ta_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensor_data")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_STARTUP_RETRIES = int(os.getenv("INFLUX_STARTUP_RETRIES", "30"))
INFLUX_STARTUP_DELAY_SEC = float(os.getenv("INFLUX_STARTUP_DELAY_SEC", "2"))


def sanitize_key(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    return sanitized or "unknown"


def flatten_payload(value, prefix="", output=None):
    if output is None:
        output = {}

    if isinstance(value, dict):
        for key, nested_value in value.items():
            nested_prefix = f"{prefix}_{sanitize_key(str(key))}" if prefix else sanitize_key(str(key))
            flatten_payload(nested_value, nested_prefix, output)
        return output

    if isinstance(value, list):
        for index, nested_value in enumerate(value):
            nested_prefix = f"{prefix}_{index}" if prefix else str(index)
            flatten_payload(nested_value, nested_prefix, output)
        return output

    if prefix:
        output[prefix] = value

    return output


def parse_timestamp(value):
    if isinstance(value, bool) or value is None:
        return None

    if isinstance(value, (int, float)):
        numeric_value = float(value)
        abs_value = abs(numeric_value)

        # InfluxDB/Python handles epoch in seconds. 
        # If the value is too small (e.g. < 10^9), it's likely not a valid epoch 
        # but rather an uptime or relative counter. We ignore it to use server time.
        if abs_value < 1_000_000_000:
            return None

        if abs_value >= 1e17: # Nanoseconds
            return datetime.fromtimestamp(numeric_value / 1_000_000_000, tz=timezone.utc)
        if abs_value >= 1e14: # Microseconds
            return datetime.fromtimestamp(numeric_value / 1_000_000, tz=timezone.utc)
        if abs_value >= 1e11: # Milliseconds
            return datetime.fromtimestamp(numeric_value / 1_000, tz=timezone.utc)
        return datetime.fromtimestamp(numeric_value, tz=timezone.utc)

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    return None


def extract_event_time(fields):
    for candidate in ("timestamp", "ts", "time", "event_time"):
        if candidate in fields:
            parsed = parse_timestamp(fields[candidate])
            if parsed is not None:
                return parsed
    return None


def measurement_from_topic(topic: str) -> str:
    segments = [sanitize_key(part) for part in topic.split("/") if part]
    if not segments:
        return "mqtt"
    # Use only the first segment as measurement name to group related data
    return segments[0]


def tags_from_topic(topic: str, fields: dict) -> dict:
    segments = [part for part in topic.split("/") if part]
    tags = {"mqtt_topic": topic}

    for index, segment in enumerate(segments):
        tags[f"topic_level_{index}"] = segment

    # Add useful fields as tags if they exist
    for field_name in ("nodeId", "node_id", "status", "device_id"):
        if field_name in fields and fields[field_name] is not None:
            tags[sanitize_key(field_name)] = str(fields[field_name])

    return tags


def build_point(topic: str, payload: dict):
    fields = flatten_payload(payload)
    event_time = extract_event_time(fields)
    measurement = measurement_from_topic(topic)
    tags = tags_from_topic(topic, fields)

    point = Point(measurement)

    for tag_key, tag_value in tags.items():
        point.tag(sanitize_key(tag_key), str(tag_value))

    field_count = 0
    for field_key, field_value in fields.items():
        if field_value is None:
            continue

        sanitized_key = sanitize_key(field_key)

        if isinstance(field_value, bool):
            point.field(sanitized_key, field_value)
            field_count += 1
            continue

        if isinstance(field_value, int):
            point.field(sanitized_key, field_value)
            field_count += 1
            continue

        if isinstance(field_value, float):
            if math.isnan(field_value) or math.isinf(field_value):
                continue
            point.field(sanitized_key, field_value)
            field_count += 1
            continue

        if isinstance(field_value, str):
            point.field(sanitized_key, field_value)
            field_count += 1

    if field_count == 0:
        return None

    if event_time is not None:
        point.time(event_time, WritePrecision.NS)

    return point


if not INFLUX_TOKEN:
    raise SystemExit("INFLUX_TOKEN is required for the MQTT to Influx bridge.")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)


def wait_for_influx():
    for attempt in range(1, INFLUX_STARTUP_RETRIES + 1):
        try:
            if influx_client.ping():
                logging.info("InfluxDB is ready")
                return
            logging.warning(
                "InfluxDB ping attempt %s/%s returned unhealthy status",
                attempt,
                INFLUX_STARTUP_RETRIES,
            )
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "InfluxDB ping attempt %s/%s failed: %s",
                attempt,
                INFLUX_STARTUP_RETRIES,
                exc,
            )

        if attempt < INFLUX_STARTUP_RETRIES:
            time.sleep(INFLUX_STARTUP_DELAY_SEC)

    raise SystemExit(
        "InfluxDB is not ready after "
        f"{INFLUX_STARTUP_RETRIES} attempts "
        f"(delay {INFLUX_STARTUP_DELAY_SEC}s)."
    )


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logging.info("Connected to MQTT broker at %s:%s (Transport: %s)", MQTT_HOST, MQTT_PORT, MQTT_TRANSPORT)
        client.subscribe(MQTT_TOPIC)
        logging.info("Subscribed to topic filter: %s", MQTT_TOPIC)
        return

    logging.error("MQTT connection failed with reason code %s", reason_code)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    logging.warning("Disconnected from MQTT broker with reason code %s", reason_code)


def on_message(client, userdata, message):
    payload_text = message.payload.decode("utf-8", errors="replace").strip()
    logging.info("Received MQTT message on topic: %s", message.topic)

    if not payload_text:
        logging.warning("Skipping empty payload on topic %s", message.topic)
        return

    if message.topic.startswith("$SYS/"):
        return

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        # If not JSON, treat as raw data to ensure nothing is lost
        logging.info("Non-JSON payload on topic %s, storing as raw_data", message.topic)
        payload = {"raw_data": payload_text}

    if not isinstance(payload, dict):
        payload = {"value": payload}

    point = build_point(message.topic, payload)
    if point is None:
        logging.warning("Skipping payload without writable fields on topic %s", message.topic)
        return

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logging.info("Successfully wrote point for topic %s to InfluxDB", message.topic)
    except Exception as exc:  # noqa: BLE001
        logging.error("Failed writing topic %s to InfluxDB: %s", message.topic, exc)


def main():
    logging.info("Starting MQTT to Influx bridge")
    logging.info("InfluxDB URL=%s org=%s bucket=%s", INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET)
    wait_for_influx()

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2, 
        client_id=MQTT_CLIENT_ID,
        transport=MQTT_TRANSPORT
    )
    client.enable_logger()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    main()
