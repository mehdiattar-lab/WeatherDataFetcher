#!/usr/bin/env python3
import argparse, json, sys
import paho.mqtt.client as mqtt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="broker")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--topic", default="#")
    ap.add_argument("--username")
    ap.add_argument("--password")
    ap.add_argument("--tls", action="store_true")
    args = ap.parse_args()

    client = mqtt.Client(client_id="mqtt-printer",
                         callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if args.username:
        client.username_pw_set(args.username, args.password)
    if args.tls:
        client.tls_set()
    client.reconnect_delay_set(1, 30)

    def on_connect(c, u, flags, reason_code, properties):
        c.subscribe(args.topic, qos=1)

    def on_message(c, u, msg):
        try:
            obj = json.loads(msg.payload.decode("utf-8"))
            print(json.dumps(obj, ensure_ascii=False), flush=True)
        except Exception:
            print(msg.payload.decode("utf-8", errors="replace"), flush=True)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect_async(args.host, args.port, keepalive=60)
    try:
        client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e), file=sys.stderr, flush=True)
        sys.exit(1)
