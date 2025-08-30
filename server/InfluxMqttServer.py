import json

import random

from time import sleep

from influxdb_client import Point, WritePrecision
from influxdb_client.rest import ApiException
from paho.mqtt.client import MQTTMessage



from server.MqttClient import MqttClient
from InfluxClient import InfluxClient





MQTT_SERVER = "192.168.1.200"
#MQTT_SERVER = "104.53.51.51"
MQTT_USER   = "delta"
MQTT_PWD    = "KeepClimbing!"

INFLUX_SERVER = "http://192.168.1.200:8086"
#INFLUX_SERVER = "http://104.53.51.51:8086"
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

        self.mqttClient.subscribe("Delta/N304DL/Viasat",lambda m,u: u.process(m),self)


    def parse(self,m:MQTTMessage) :
        asJson = None

        try:
            asJson = json.loads(m.payload)

        except json.JSONDecodeError as jde:
            pass

        return asJson

    def cvtToPoint(self,topic:str,data) -> Point :
        p = None
        ts = None

        tpc = topic.split('/')

        try:
            tmp = Point(tpc[1])
            if len(tpc) > 2:
                tmp.tag("Source",tpc[2])
            tmp.tag("regNum",data['aircraft']['tailNum'])
            tmp.tag("fltNum",data['aircraft']['flightNum'])

            for k in data['data'].keys():
                tmp.field(k,data['data'][k])

            ts = data['aircraft']['timestamp']
            tmp = tmp.time(ts,write_precision=WritePrecision.S)
            p = tmp

        except KeyError as ke:
            pass

        return p,ts

    def process(self,m:MQTTMessage) :
        payload = self.parse(m)

        if (payload is not None) :
            print(payload)
            pt,ts = self.cvtToPoint(m.topic,payload)
            if (pt is not None) :
                self.dbase.write(ts,pt)



    def run(self) :
        self.init()

        self.mqttClient.mqClient.loop_start()


    def terminate(self) :
        self.mqttClient.terminate()




if __name__ == "__main__" :
    db = InfluxClient(INFLUX_SERVER,"Flight",org="Brian Still",token=INFLUX_APIKEY)

    mqtt = MqttClient(MQTT_SERVER,1883,user=MQTT_USER,passwd=MQTT_PWD,
                      clientID=f"infdb-{random.randint(0,10_000):06d}")
    mqtt.run()

    mqClient = InfluxMqttServer(db, mqtt)

    pub = MqttClient(MQTT_SERVER,1883,user=MQTT_USER,passwd=MQTT_PWD,
                     clientID=f"test-{random.randint(0,10_000)}")
    pub.run()


    def genTestMsg() :
        data = {"alt": random.uniform(-100, 40_000) ,
                "lat": random.uniform(-90, 90),
                "lon": random.uniform(-180, 180) }


        data = {"aircraft": {"tailNum": "TEST", "flightNum": "###", "timestamp": "2025-08-30T14:28:09+00:00"}, "timestamp": "2025-08-30 10:28:09.281861", "data": {"latitude": 30.4469485748939, "longitude": -99.234234, "altitude": 1715.0, "groundSpd": 413.0, "heading": 13.0, "airspeed": 410.0}}
        msg = json.dumps(data)

        return msg


    try:
        mqClient.run()

        while True:
            msg = genTestMsg()

            pub.publish("Delta/N304DL/Viasat",msg)
            sleep(1)

    except ApiException as e :
        pass
    except KeyboardInterrupt as e :
        mqClient.terminate()
        pub.terminate()






