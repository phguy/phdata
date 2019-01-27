#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 12:23:07 2018

@author: joe

Loops thru:
    -receiving raw CAN-bus bytes via mqtt, containing measurements
    -decoding to CAN-ID specific human readable message 
    -publishing as mqtt message with topic derived from 
"""

_VERSION= '201812119.1'

import sys
import json
import paho.mqtt.client as mqtt
from candata2values import decode_mqtt

MQTT_SERVER= 'hassio.fritz.box'
MQTT_USER=   'mqttuser'
MQTT_PASSWD= 'tel29770mr'
MQTT_TOPIC_IN=  '/phdata/solvis/can/DLC:8' #8-byte message
MQTT_TOPIC_OUT= '/phdata/solvis/msgs'

def on_message(client, userdata, message):
    msg= str(message.payload.decode("utf-8"))
    print('message received: "{}"'.format(msg))
    decoded= decode_mqtt(msg)
    if decoded == None:
        print('=> unable to decode, msg datalength != 8 or CAN-ID undefined')
    for dmsg in decoded:
        print(dmsg)
        client.publish(topic='{}/{}'.format(MQTT_TOPIC_OUT, dmsg['name']),
                       payload= json.dumps(dmsg))
    

def on_connect(client, userdata, flags, rc):
  print('Connected with result code {}'.format(rc))
  client.subscribe(topic= MQTT_TOPIC_IN)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print('Unexpected disconnection.code {}'.format(rc))
                   
def on_subscribe(client, userdata, mid, granted_qos):
    print('subscribed to {}, qos={}'.format(mid, granted_qos))
    
def client_close(client):
    client.disconnect()
    client.loop.stop()

def on_log(client, userdata, level, buf):
    print("log: ",buf)

def main():
    client= mqtt.Client('jacer-mqtt')
    #define callback functions
#    client.on_log= on_log
    client.on_connect=    on_connect
    client.on_disconnect= on_disconnect
    client.on_subscribe=  on_subscribe
    client.on_message=    on_message
    
    #start session
    client.username_pw_set(username= MQTT_USER, password= MQTT_PASSWD)
    client.connect_async(host=MQTT_SERVER)
    client.loop_forever()
    
if __name__ == "__main__":
  sys.exit(main())

