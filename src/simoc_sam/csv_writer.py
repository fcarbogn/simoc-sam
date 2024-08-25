import os
import csv
import json

import paho.mqtt.client as mqtt

from simoc_sam.sensors import utils


HOST = 'mqtt.simoc.space'
PORT = 8883
KEEPALIVE = 10  # in seconds
TOPIC = "lunares/#"

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc, properties=None):
    print(f'Connected with result code {rc}')
    # Subscribe to the MQTT topic
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    topic = msg.topic
    print(f"Received message: {payload}")
    print(f"from topic: {topic}")
    # Parse the payload (assuming it's JSON, adjust as needed)
    data = json.loads(payload)

    # Define file name based on topic
    csv_file_path = "/home/data/" + topic.replace("/", "_") + ".csv"

    # Append the data to the CSV file
    with open(csv_file_path, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        #field_names = sorted(data.keys())
        field_names = ['n', 'timestamp', *[k for k in data if k not in {'n', 'timestamp'}]]
        # Check if the CSV file is empty
        is_empty = os.stat(csv_file_path).st_size == 0

        # Write headers if the file is empty
        if is_empty:
            # Update the set of field names based on the payload keys
            csv_writer.writerow(field_names)

        # Write data to the CSV file
        csv_writer.writerow([data.get(field, '') for field in field_names])

    #print("Data logged to CSV")

# main

def main(host=HOST, port=PORT, topic=TOPIC):
    # Create an MQTT client
    client = mqtt.Client()

    # Set callback functions
    client.on_connect = on_connect
    client.on_message = on_message

    client.tls_set(ca_certs="/etc/mosquitto/certs/ca.crt", certfile="/etc/mosquitto/certs/client.crt", keyfile="/etc/mosquitto/certs/client.key")
    client.tls_insecure_set(True)

    # Connect to the MQTT broker
    client.connect(host, port, KEEPALIVE)

    # Loop to handle MQTT communication
    client.loop_forever()
    print("Disconnected from MQTT broker")

if __name__ == '__main__':
    main()
