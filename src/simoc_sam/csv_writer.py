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
