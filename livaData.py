import sys
import paho.mqtt.client as mqtt
import traceback
import requests
import json
import time
import platform
from dataMigrationImpl import config,preProdconfig
sourceUnitsId = "628dd242c78e4c5d0f3b90cf"
destUnitId = "628dd242c78e4c5d0f3b90cf"

def on_message(client, userdata, msg):
    # client2.publish(msg.topic,msg.payload)
    topic = msg.topic.replace(sourceUnitsId,destUnitId)
    body = json.loads(msg.payload)
    newTag = topic.split("/")[2]
    client2.publish(topic,msg.payload)
    print(body)
    try:
        postBody = [{"name":newTag,"datapoints":[[body[0]["t"],body[0]["v"]]],"tags": {"type":"raw"}}]
        print(postBody)
        client2.publish("kairoswriteexternal",json.dumps(postBody))
    except:
        # postBody = [{"name":newTag,"datapoints":[[body[0]["t"],body[0]["v"]]],"tags": {"type":"raw"}}]
        postBody = [{"name":newTag,"datapoints":[[body[0]["t"],body[0]["r"]]],"tags": {"type":"raw"}}]
        print(postBody)
        client2.publish("kairoswriteexternal",json.dumps(postBody))
        pass
   


def on_connect(client, userdata, flags, rc):
    client.subscribe(f"u/{sourceUnitsId}/+/r")

def on_message2(client, userdata, msg):
    # print(msg.payload)
    body = json.loads(msg.payload)
    # print(body)

def on_connect2(client, userdata, flags, rc):
    client.subscribe(f"u/{destUnitId}/+/r")



def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))
    # pass

BROKER_ADDRESS = config["BROKER_ADDRESS"] #(SUBSCRIBER broker address)
print("subscribing address: ",BROKER_ADDRESS)
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client.username_pw_set(username=username, password=password)
except:
    pass

client.connect(BROKER_ADDRESS, 1883, 2800)

BROKER_ADDRESS = preProdconfig["BROKER_ADDRESS"] #(PUBLISHER broker address)
print("publishing address: ",BROKER_ADDRESS)

client2 = mqtt.Client(client_id="es-user")
client2.on_connect = on_connect2
client2.on_message = on_message2
client2.on_log = on_log
try:
    username = preProdconfig["BROKER_USERNAME"]
    password = preProdconfig["BROKER_PASSWORD"]
    client2.username_pw_set(username=username, password=password)
except:
    pass

client2.connect(BROKER_ADDRESS, 1883, 2800)
client2.loop_start()
client.loop_forever()