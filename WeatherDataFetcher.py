#!/usr/bin/env python3
"""
FMI minute measurements + hourly forecasts -> MQTT publisher

- Every minute:
    - Publishes Temperature measurement message
    - Publishes Irradiance  measurement message
- At the start of each UTC hour (hh:00):
    - Also publishes 36h Temperature forecast message
    - Also publishes 36h Irradiance  forecast message

Quick test (one cycle: measurements now + forecasts once):
  python WeatherDataFetcher.py --once --place "Helsinki"

Run continuously (default topics + MQTT settings from USER CONFIG):
  python WeatherDataFetcher.py --place "Helsinki"
  # or with coordinates:
  python WeatherDataFetcher.py --lat 60.1699 --lon 24.9384
"""

# =========================== USER CONFIG ======================================

# ---- MQTT connection ----
MQTT_BROKER   = "localhost"   # e.g. "127.0.0.1" or "mqtt.my.net"
MQTT_PORT     = 1883
MQTT_USERNAME = None          # or "user"
MQTT_PASSWORD = None          # or "pass"
MQTT_TLS      = False         # True to enable TLS (uses default CA)
MQTT_CLIENT_ID = "fmi-forecaster-1"
MQTT_KEEPALIVE = 60

# Publish settings
MQTT_QOS    = 1
MQTT_RETAIN = False

# Optional: also print full JSON payloads to terminal for debugging
PRINT_JSON_TO_STDOUT = False

# ---- Topics (customize these) ----
TOPIC_TEMP_MEAS = "Measurement/Temperature"
TOPIC_IRR_MEAS  = "Measurement/Irradiance"
TOPIC_TEMP_FC   = "Measurement/Forecast/Temperature"
TOPIC_IRR_FC    = "Measurement/Forecast/Irradiance"

# --- ENV OVERRIDES (add after USER CONFIG constants) ---
import os

MQTT_BROKER   = os.getenv("MQTT_BROKER", MQTT_BROKER)
MQTT_PORT     = int(os.getenv("MQTT_PORT", MQTT_PORT))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", MQTT_USERNAME or "") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", MQTT_PASSWORD or "") or None
MQTT_TLS      = os.getenv("MQTT_TLS", "false").lower() in ("1","true","yes")

MQTT_QOS      = int(os.getenv("MQTT_QOS", MQTT_QOS))
MQTT_RETAIN   = os.getenv("MQTT_RETAIN", str(MQTT_RETAIN)).lower() in ("1","true","yes")

TOPIC_TEMP_MEAS = os.getenv("TOPIC_TEMP_MEAS", TOPIC_TEMP_MEAS)
TOPIC_IRR_MEAS  = os.getenv("TOPIC_IRR_MEAS", TOPIC_IRR_MEAS)
TOPIC_TEMP_FC   = os.getenv("TOPIC_TEMP_FC", TOPIC_TEMP_FC)
TOPIC_IRR_FC    = os.getenv("TOPIC_IRR_FC", TOPIC_IRR_FC)

# ==============================================================================
import argparse
import json
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs

import requests
import xml.etree.ElementTree as ET

# MQTT lib
try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.stderr.write("paho-mqtt is required. Install with: pip install paho-mqtt\n")
    sys.exit(1)

# --- FMI WFS setup ------------------------------------------------------------

WFS = "https://opendata.fmi.fi/wfs"
NS = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "om": "http://www.opengis.net/om/2.0",
    "omso": "http://inspire.ec.europa.eu/schemas/omso/3.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "wml2": "http://www.opengis.net/waterml/2.0",
    "xlink": "http://www.w3.org/1999/xlink",
    "target": "http://xml.fmi.fi/namespace/om/atmosphericfeatures/1.1",
}

# Stored queries / parameters
OBS_WEATHER_SQ = "fmi::observations::weather::timevaluepair"      # t2m
OBS_RADIATION_SQ = "fmi::observations::radiation::timevaluepair"  # GLOB_1MIN
FC_HARMONIE_SQ = "fmi::forecast::harmonie::surface::point::timevaluepair"

PARAM_T_OBS = "t2m"                 # °C (observations, usually 10-min cadence)
PARAM_GLOB_OBS = "GLOB_1MIN"        # W/m2 (observations, 1-min)
PARAM_T_FC = "temperature"          # °C (forecast, hourly)
PARAM_GLOB_FC = "RadiationGlobal"   # W/m2 (forecast, hourly)

# --- Small helpers ------------------------------------------------------------

def iso_z(dt: datetime, timespec: str = "seconds") -> str:
    """UTC ISO-8601 string with trailing Z."""
    return dt.astimezone(timezone.utc).isoformat(timespec=timespec).replace("+00:00", "Z")

def ceil_to_minute(dt: datetime) -> datetime:
    base = dt.replace(second=0, microsecond=0)
    return base if dt == base else base + timedelta(minutes=1)

def ceil_to_hour(dt: datetime) -> datetime:
    base = dt.replace(minute=0, second=0, microsecond=0)
    return base if dt == base else base + timedelta(hours=1)

def wfs_get(storedquery_id: str, **params) -> bytes:
    q = {"service": "WFS", "version": "2.0.0", "request": "getFeature", "storedquery_id": storedquery_id}
    q.update(params)
    r = requests.get(WFS, params=q, timeout=30)
    r.raise_for_status()
    return r.content

def parse_timevaluepairs(xml_bytes: bytes) -> List[Dict[str, Any]]:
    """Return list of dicts: param, time (UTC), value (float)"""
    root = ET.fromstring(xml_bytes)
    out: List[Dict[str, Any]] = []
    for member in root.findall(".//wfs:member", NS):
        prop = member.find(".//om:observedProperty", NS)
        href = prop.get("{%s}href" % NS["xlink"]) if prop is not None else ""
        param_code = ""
        if href:
            try:
                param_code = parse_qs(urlparse(href).query).get("param", [""])[0]
            except Exception:
                pass
        for tvp in member.findall(".//wml2:MeasurementTVP", NS):
            t_el = tvp.find("wml2:time", NS)
            v_el = tvp.find("wml2:value", NS)
            if t_el is None or v_el is None or v_el.text is None:
                continue
            try:
                t = datetime.fromisoformat(t_el.text.replace("Z", "+00:00")).astimezone(timezone.utc)
                v = float(v_el.text)
            except Exception:
                continue
            out.append({"param": param_code, "time": t, "value": v})
    return out

def latest_value(series: List[Dict[str, Any]], param: str) -> Optional[Dict[str, Any]]:
    vals = [row for row in series if row["param"] == param and row["value"] == row["value"]]
    if not vals:
        return None
    return max(vals, key=lambda r: r["time"])

def bucket_hourly(series: List[Dict[str, Any]], want_param: str) -> Dict[datetime, float]:
    """Map hour -> value for param (keeps last if duplicates)."""
    d: Dict[datetime, float] = {}
    for r in series:
        if r["param"] == want_param:
            t = r["time"].replace(minute=0, second=0, microsecond=0)
            d[t] = r["value"]
    return d

# --- Data fetchers ------------------------------------------------------------

def fetch_latest_measurements(loc: Dict[str, str]) -> Dict[str, Optional[Dict[str, Any]]]:
    """Pull recent observations and return the latest for t2m and GLOB_1MIN."""
    now_utc = datetime.now(timezone.utc)
    start = (now_utc - timedelta(minutes=90))
    obs_t_xml = wfs_get(
        OBS_WEATHER_SQ,
        parameters=PARAM_T_OBS,
        starttime=iso_z(start),
        endtime=iso_z(now_utc),
        **loc,
    )
    obs_g_xml = wfs_get(
        OBS_RADIATION_SQ,
        parameters=PARAM_GLOB_OBS,
        starttime=iso_z(start),
        endtime=iso_z(now_utc),
        **loc,
    )
    obs_t = parse_timevaluepairs(obs_t_xml)
    obs_g = parse_timevaluepairs(obs_g_xml)
    return {
        "t": latest_value(obs_t, PARAM_T_OBS),
        "g": latest_value(obs_g, PARAM_GLOB_OBS),
    }

def fetch_hourly_forecast(loc: Dict[str, str], hours: int) -> Dict[str, Any]:
    """Pull forecast from next full hour for 'hours' hours, return hourly arrays."""
    now_utc = datetime.now(timezone.utc)
    start = ceil_to_hour(now_utc)
    end = start + timedelta(hours=hours)
    fc_xml = wfs_get(
        FC_HARMONIE_SQ,
        parameters=",".join([PARAM_T_FC, PARAM_GLOB_FC]),
        starttime=iso_z(start),
        endtime=iso_z(end),
        timestep=60,  # hourly
        **loc,
    )
    fc = parse_timevaluepairs(fc_xml)
    temp_by_hour = bucket_hourly(fc, PARAM_T_FC)
    irr_by_hour = bucket_hourly(fc, PARAM_GLOB_FC)
    hourly_times = [start + timedelta(hours=h) for h in range(hours)]
    return {
        "times": hourly_times,
        "temp_vals": [temp_by_hour.get(t) for t in hourly_times],
        "irr_vals": [irr_by_hour.get(t) for t in hourly_times],
        "start": start,
    }

# --- Message builders ----------------------------------------------------------

def build_temp_measurement_msg(topic: str, location_str: str, latest_t: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    msg_id = iso_z(datetime.now(timezone.utc), "milliseconds")
    if latest_t:
        ts = iso_z(latest_t["time"], "milliseconds")
        val = latest_t["value"]
    else:
        ts = iso_z(datetime.now(timezone.utc), "milliseconds")
        val = None
    return {
        "MessageId": msg_id,
        "Timestamp": ts,
        "Temperature": {"Value": val, "Unit": "Cel"},
        "Topic": topic,
        "Location": location_str,
    }

def build_irr_measurement_msg(topic: str, location_str: str, latest_g: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    msg_id = iso_z(datetime.now(timezone.utc), "milliseconds")
    if latest_g:
        ts = iso_z(latest_g["time"], "milliseconds")
        val = latest_g["value"]
    else:
        ts = iso_z(datetime.now(timezone.utc), "milliseconds")
        val = None
    return {
        "MessageId": msg_id,
        "Timestamp": ts,
        "Irradiance": {"Value": val, "Unit": "W/m2"},
        "Topic": topic,
        "Location": location_str,
    }

def build_forecast_msg(series_name: str, unit: str, topic: str, location_str: str,
                       times: List[datetime], values: List[Optional[float]]) -> Dict[str, Any]:
    msg_id = iso_z(datetime.now(timezone.utc), "milliseconds")
    return {
        "MessageId": msg_id,
        "Forecast": {
            "TimeIndex": [iso_z(t) for t in times],
            "Series": {
                series_name: {
                    "UnitOfMeasure": unit,
                    "Values": values
                }
            }
        },
        "Topic": topic,
        "Location": location_str,
    }

# --- MQTT glue ----------------------------------------------------------------

_connected_evt = threading.Event()

def make_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS:
        client.tls_set()  # use system CAs; adjust as needed
    # LWT is optional; handy if a dashboard wants online/offline
    client.will_set("WeatherMeasurement/status", payload="offline", qos=1, retain=True)

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            _connected_evt.set()
            # Announce online
            client.publish("WeatherMeasurement/status", payload="online", qos=1, retain=True)
            print(f"[MQTT] Connected to {MQTT_BROKER}:{MQTT_PORT}")
        else:
            print(f"[MQTT] Connect failed with rc={rc}")

    def on_disconnect(client, userdata, rc, properties=None):
        _connected_evt.clear()
        print(f"[MQTT] Disconnected (rc={rc}). Reconnecting…")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Start networking thread and connect (async -> auto reconnect)
    client.loop_start()
    client.connect_async(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    return client

def publish_json(client: mqtt.Client, topic: str, message: Dict[str, Any]):
    """Ensure Topic field, publish JSON to MQTT, and optionally print payload."""
    message["Topic"] = topic  # guarantee correctness
    payload = json.dumps(message, ensure_ascii=False)
    # Wait for connection (non-blocking retry if broker not up yet)
    if not _connected_evt.is_set():
        # wait a short time the first time; background thread will reconnect
        _connected_evt.wait(timeout=5.0)
    rc = client.publish(topic, payload=payload, qos=MQTT_QOS, retain=MQTT_RETAIN)[0]
    if rc != mqtt.MQTT_ERR_SUCCESS:
        sys.stderr.write(f"[MQTT] Publish failed rc={rc} on topic {topic}\n")
    else:
        print(f"[MQTT] -> {topic} ({len(payload)} bytes)")
        if PRINT_JSON_TO_STDOUT:
            print(payload)

# --- Runner -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=False)
    g.add_argument("--place", help="Place name (e.g., Helsinki)")
    ap.add_argument("--lat", type=float)
    ap.add_argument("--lon", type=float)
    ap.add_argument("--hours", type=int, default=36, help="Forecast horizon (hours)")
    ap.add_argument("--once", action="store_true", help="Publish measurements now and forecasts once, then exit")
    args = ap.parse_args()

    # Location selection
    DEFAULT_PLACE = "Hirvensalmi"
    if args.lat is not None and args.lon is not None:
        loc = {"latlon": f"{args.lat:.6f},{args.lon:.6f}"}
        location_str = loc["latlon"]
    else:
        loc = {"place": args.place or DEFAULT_PLACE}
        location_str = loc["place"]

    # MQTT client
    client = make_mqtt_client()

    # --- One-shot mode --------------------------------------------------------
    if args.once:
        try:
            latest = fetch_latest_measurements(loc)
            temp_msg = build_temp_measurement_msg(TOPIC_TEMP_MEAS, location_str, latest["t"])
            irr_msg  = build_irr_measurement_msg(TOPIC_IRR_MEAS, location_str, latest["g"])
            publish_json(client, TOPIC_TEMP_MEAS, temp_msg)
            publish_json(client, TOPIC_IRR_MEAS, irr_msg)

            fc = fetch_hourly_forecast(loc, args.hours)
            temp_fc_msg = build_forecast_msg("Temperature", "Cel", TOPIC_TEMP_FC, location_str, fc["times"], fc["temp_vals"])
            irr_fc_msg  = build_forecast_msg("Irradiance", "W/m2", TOPIC_IRR_FC, location_str, fc["times"], fc["irr_vals"])
            publish_json(client, TOPIC_TEMP_FC, temp_fc_msg)
            publish_json(client, TOPIC_IRR_FC, irr_fc_msg)
        except requests.HTTPError as e:
            sys.stderr.write(f"HTTP error from FMI: {e}\n")
            sys.exit(1)
        except requests.RequestException as e:
            sys.stderr.write(f"Network error: {e}\n")
            sys.exit(1)
        finally:
            time.sleep(0.2)  # let async network flush
            client.loop_stop()
            client.disconnect()
        return

    # --- Continuous mode ------------------------------------------------------
    last_forecast_hour: Optional[int] = None

    try:
        while True:
            # sleep until next minute boundary
            now = datetime.now(timezone.utc)
            wake = ceil_to_minute(now)
            time.sleep((wake - now).total_seconds())

            # 1) Measurements (every minute)
            try:
                latest = fetch_latest_measurements(loc)
                temp_msg = build_temp_measurement_msg(TOPIC_TEMP_MEAS, location_str, latest["t"])
                irr_msg  = build_irr_measurement_msg(TOPIC_IRR_MEAS, location_str, latest["g"])
                publish_json(client, TOPIC_TEMP_MEAS, temp_msg)
                publish_json(client, TOPIC_IRR_MEAS,  irr_msg)
            except requests.HTTPError as e:
                sys.stderr.write(f"HTTP error from FMI (observations): {e}\n")
            except requests.RequestException as e:
                sys.stderr.write(f"Network error (observations): {e}\n")

            # 2) Forecasts (once per hour at hh:00)
            now = datetime.now(timezone.utc)
            if now.minute == 0 and now.hour != last_forecast_hour:
                last_forecast_hour = now.hour
                try:
                    fc = fetch_hourly_forecast(loc, args.hours)
                    temp_fc_msg = build_forecast_msg("Temperature", "Cel", TOPIC_TEMP_FC, location_str, fc["times"], fc["temp_vals"])
                    irr_fc_msg  = build_forecast_msg("Irradiance", "W/m2", TOPIC_IRR_FC, location_str, fc["times"], fc["irr_vals"])
                    publish_json(client, TOPIC_TEMP_FC, temp_fc_msg)
                    publish_json(client, TOPIC_IRR_FC,  irr_fc_msg)
                except requests.HTTPError as e:
                    sys.stderr.write(f"HTTP error from FMI (forecast): {e}\n")
                except requests.RequestException as e:
                    sys.stderr.write(f"Network error (forecast): {e}\n")
    except KeyboardInterrupt:
        pass
    finally:
        time.sleep(0.2)  # allow pending publishes to flush
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.stderr.write(f"Fatal error: {e}\n")
        sys.exit(1)
