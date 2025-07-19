#!/usr/bin/env python
import paho.mqtt.client as mqtt
import json

KEEPALIVE = 10  # in seconds
TOPIC = "#"

REMOTE_BROKER ='85.214.10.8'
REMOTE_PORT = 1883

LOCAL_BROKER ='mqtt.simoc.space' # this must match the CNAME in your server-cert!
LOCAL_PORT = 8883
remote_clients = {}
# Callback when the client connects to the broker
def on_local_connect(local_client, userdata, flags, rc, properties=None):
    print(f'Locally connected with result code {rc}')
    # Subscribe to the MQTT topic
    local_client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    topic = msg.topic
    print(f"Received message: {payload}")
    print(f"from topic: {topic}")
    forward_message(topic, payload)


def forward_message(topic, payload):
    parts = topic.split('/')
    client_id = "_".join(parts[:2])
    if client_id not in remote_clients:
        client = mqtt.Client(client_id=client_id)
        client.connect(REMOTE_BROKER, REMOTE_PORT, KEEPALIVE)
        client.loop_start()
        remote_clients[client_id] = client
    else:
        client = remote_clients[client_id]
    client.publish(topic, payload)

# Create an MQTT client
local_client = mqtt.Client()

# Set callback functions
local_client.on_connect = on_local_connect
local_client.on_message = on_message

local_client.tls_set(ca_certs="/etc/mosquitto/certs/ca.crt", certfile="/etc/mosquitto/certs/client.crt", keyfile="/etc/mosquitto/certs/client.key")
local_client.tls_insecure_set(True)

# Connect to the local MQTT broker
local_client.connect(LOCAL_BROKER, LOCAL_PORT, KEEPALIVE)
# Connect to the remote MQTT broker

# Start the loop to process received messages and maintain connections
local_client.loop_start()

try:
    while True:
        pass  # Keep the script running
except KeyboardInterrupt:
    print("Exiting bridge server...")
    local_client.loop_stop()
    local_client.disconnect()
    for client in remote_clients.values():
        client.loop_stop()
        client.disconnect()
