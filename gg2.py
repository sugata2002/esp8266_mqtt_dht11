
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import datetime
import time
import paho.mqtt.client as mqtt
from datetime import datetime
import pymongo
from dotenv import load_dotenv
import os
from pymongo.server_api import ServerApi

load_dotenv()


mongo_uri = os.getenv("mongo_connection_url")
client = mqtt.Client()
# Create a new client and connect to the server
mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
broker_address = "broker.emqx.io"
port = 1883
topic = "esp8266/dht"
username = "sugata"
password = "public"
db = mongo_client["esp8266_mqtt_dht11"]
collection = db["temperature_humidity"]

# Send a ping to confirm a successful connection
try:
    mongo_client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
    
except Exception as e:
    print(e)


client.username_pw_set(username, password)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to the MQTT broker")
        client.subscribe(topic)
    else:
        print("Connection failed with result code", rc)

def on_message(client, userdata, msg):
    payload_str = msg.payload.decode('utf-8')
    # print(f"Received message on topic {msg.topic}: {payload_str}")
    if "Temperature" in payload_str:
        temperature = (payload_str)
        # humidity = (payload_str)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f" {temperature}")
        data = {
                "data": temperature,
                # "humidity": humidity,
                "timestamp": current_time
            }
        collection.insert_one(data)
        print(f"Temperature: {temperature} at {current_time}")
        print("Data inserted into MongoDB")

# Set callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(broker_address, port, 60)

# Start the MQTT loop
client.loop_start()

try:
    while True:
        time.sleep(1) 
except KeyboardInterrupt:
    print("Disconnected from the MQTT broker")
    client.disconnect()