import json
import os

import random
import sys
from argparse import Namespace, ArgumentParser
from datetime import datetime

from time import sleep

from influxdb_client import Point, WritePrecision
from influxdb_client.rest import ApiException
from paho.mqtt.client import MQTTMessage



from MqttClient import MqttClient
from InfluxClient import InfluxClient





#MQTT_SERVER = "192.168.1.200"
#MQTT_SERVER = "104.53.51.51"
#MQTT_USER   = "delta"
#MQTT_PWD    = "KeepClimbing!"

#INFLUX_SERVER = "http://192.168.1.200:8086"
#INFLUX_SERVER = "http://104.53.51.51:8086"
INFLUX_APIKEY = "kwSpQ_8q-6cwAHgquFLhp6URqaq7134ROpHEUHMDLulH49GmU1OKdS2vXb0vB7VvdxZikGp_0RiGPc7Rk9kgrw=="


import logging
logger = logging.getLogger("InfluxMqttServer")





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
        logger.info(f"Successfully created bucket")

        topic = "Delta/N304DL/ViaSat"
        self.mqttClient.subscribe(topic,lambda m,u: u.process(m),self)
        logger.info(f"Subscribed to: '{topic}'")

    def parse(self,m:MQTTMessage) :
        asJson = None

        try:
            asJson = json.loads(m.payload)
            logger.info(f"Parsed Received Message: {m.payload}")
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
            logger.info(f"Successfully converted to Influx Point")
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
                logger.info(f"Wrote Point to InfluxDB")



    def run(self) :
        self.init()

        logger.info(f"Starting MQTT Subscriber/Listener")
        self.mqttClient.mqClient.loop_start()


    def terminate(self) :
        logger.info(f"Terminating MQTT Client/Listener")
        self.mqttClient.terminate()


def parseCmdLine(args) -> Namespace :

    parser = ArgumentParser("MQTT Server that writes Aircraft data to InfluxDB")

    parser.add_argument("-v", "--verbose", action="store_true",
                        help="specify output verbosity")
    parser.add_argument("-L,--logdir",
                        dest='logDir',
                        default="/mnt/data",
                        help="specify log file for GPS receiver")

    parser.add_argument("-l,--log",
                        dest='logLevel',
                        default=logging.WARNING,
                        help="specify log level")

    parser.add_argument("-T,--test",
                        dest='testMode',
                        default=False,
                        action="store_true",
                        help="Enable Test Mode")

    parser.add_argument("-i,--influx-server",
                        dest='influxServer',
                        default="http://localhost:8086",
                        help="specify influx server")

    parser.add_argument("-B,--bucket",
                       dest="bucket",
                       default="Aircraft",
                       help="InfluxDB Bucket")



    parser.add_argument("-b,--mqtt-broker",
                        dest='mqttBroker',
                        default="localhost",
                        help="MQTT Broker URL")
    parser.add_argument("-u,--user",
                        dest="user",
                        default="delta",
                        help="MQTT User Name")

    parser.add_argument("-p,--password",
                        dest="password",
                        default="KeepClimbing!",
                        help="MQTT Password")

    return parser.parse_args(args)


if __name__ == "__main__" :

    params = parseCmdLine(sys.argv[1:])
    try:
        level = getattr(logging, params.logLevel.upper())
    except AttributeError as ae:
        level = logging.ERROR


    logfile = os.path.join(params.logDir,datetime.now().strftime("InfluxMqtt_%Y%m%d_%H%M%S.log"))
    logging.basicConfig(filename=logfile, encoding='utf-8', level=level)

    db = InfluxClient(params.influxServer,params.bucket,org="Brian Still",token=INFLUX_APIKEY)

    mqtt = MqttClient(params.mqttBroker,1883,user=params.user,passwd=params.password,
                      clientID=f"infdb-{random.randint(0,10_000):06d}")
    mqtt.run()

    mqClient = InfluxMqttServer(db, mqtt)

    pub = MqttClient(params.mqttBroker,1883,user=params.user,passwd=params.password,
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
            if params.testMode:
                logger.info(f"Generating Test message")
                msg = genTestMsg()
                pub.publish("Delta/N304DL/Viasat",msg)
            sleep(5)

    except ApiException as e :
        logger.exception(e)
    except KeyboardInterrupt as e :
        logger.info("Exiting....cleanup up clients")
        mqClient.terminate()
        pub.terminate()






