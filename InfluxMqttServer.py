import json

import random

from time import sleep, time

from influxdb_client.rest import ApiException
from paho.mqtt.client import MQTTMessage


from MqttClient import MqttClient
from InfluxClient import InfluxClient


#MQTT_SERVER = "192.168.1.200"
MQTT_SERVER = "104.53.51.51"
INFLUX_SERVER = "http://192.168.1.200:8086"
INFLUX_SERVER = "http://104.53.51.51:8086"
INFLUX_APIKEY = "kwSpQ_8q-6cwAHgquFLhp6URqaq7134ROpHEUHMDLulH49GmU1OKdS2vXb0vB7VvdxZikGp_0RiGPc7Rk9kgrw=="

class InfluxMqttServer (object) :
    FIRST_RECONNECT_DELAY = 1
    RECONNECT_RATE = 2
    MAX_RECONNECT_COUNT = 12
    MAX_RECONNECT_DELAY = 60

    def __init__ (self,db:InfluxClient,sub:MqttClient):
        self.dbase = db
        self.mqttClient = sub

    def init(self) :

        self.dbase.createBucket("Aircraft")

        self.mqttClient.subscribe("Delta/N304DL",lambda m,u: u.process(m),self)


    def process(self,m:MQTTMessage) :
        print(f"Processing message: {m.topic}")

        try:
            asJson = json.loads(m.payload)
        except json.decoder.JSONDecodeError:
            return

        print(asJson)


    def run(self) :
        self.init()

        self.mqttClient.mqClient.loop_start()


    def terminate(self) :
        self.mqttClient.terminate()




if __name__ == "__main__" :
    db = InfluxClient(INFLUX_SERVER,"Flight",org="Brian Still",token=INFLUX_APIKEY)

    mqtt = MqttClient(MQTT_SERVER,1883)
    mqtt.run()

    mqClient = InfluxMqttServer(db, mqtt)

    pub = MqttClient(MQTT_SERVER,1883)
    pub.run()


    try:
        mqClient.run()

        while True:
            msg = f'{{"alt": {random.uniform(-100,40_000)} ,"lat": {random.uniform(-90,90)},"lon": {random.uniform(-180,180)} }}'
            pub.publish("Delta/N304DL",msg)
            sleep(1)

    except ApiException as e :
        pass
    except KeyboardInterrupt as e :
        mqClient.terminate()
        pub.terminate()






