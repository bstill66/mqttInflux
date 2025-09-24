import json
import os
import random
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from multiprocessing import Queue
from queue import Empty
from threading import Event, Thread
from time import sleep
from typing import Callable

from ViasatMSI import ViasatMSI
from client.MsiToOcc import transform
from common.KinesisClient import KinesisClient
from common.MqttClient import MqttClient

import logging
logger = logging.getLogger()

class AircraftClient(object) :
    def __init__(self,msi="https://msi.viasat.com:9100/v1/flight") :
        super().__init__()

        self.msiUrl = msi
        self.api = ViasatMSI(msi)
        self.startFlag = Event()
        self.clients = {}

    def addClient(self,name:str,func:Callable,client) :
        q = Queue()
        evt = Event()
        evt.set()

        thr = Thread(target=self.clientRun,args=(name,q,evt,func,client))
        self.clients[name] = (thr,q,evt,func,client)
        thr.start()


    def publishKinesis(self,msiData,aws) :
        if aws is None or not isinstance(aws,KinesisClient) :
            return

        try:
            kdata = json.dumps(transform(msiData))
            rsp = aws.publish(None,kdata)
            logger.debug(f"published {kdata} to Kinesis")
        except Exception as e:
            logger.exception(e)




    def publishMqtt(self,msiData,mqtt) :
        if mqtt is None or not isinstance(mqtt,MqttClient) :
            return

        header = {"timestamp": str(datetime.now(timezone.utc))}

        # encode the topic for better efficiency
        topic = f"Delta/{msiData['vehicleId']}/{msiData['flightNumber']}/MSI"

        # remove since encoded in the topic
        #msiData.pop('vehicleId')
        #msiData.pop('flightNumber')

        # Don't send empty data elements
        dataToSend = {}
        for k in msiData.keys() :
            if msiData[k] != None :
                dataToSend[k] = msiData[k]

        payload = {"header" : header,
                   "data" : dataToSend
                   }

        payloadStr = json.dumps(payload)


        logger.info(f"Publishing to {topic}: {payloadStr}")
        mqtt.publish(topic,payloadStr)




    def clientRun(self,name:str,msgQ:Queue,runFlag:Event,publish:Callable,client) -> None:
        logger.info(f"Client {name} waiting to start")
        self.startFlag.wait()
        logger.info(f"Client {name} started on {msgQ}")

        while runFlag.is_set() :
            try:
                data = msgQ.get(timeout=3)
                if data is not None:
                    logger.info(f"Client {name} publishing {data}")
                    publish(data,client)
                else:
                    pass
            except Empty:
                pass
            except Exception as e:
                logger.exception(e)
                pass

        logger.info(f"Client {name} exiting")

    def run(self) :
        self.startFlag.set()

        while True:
            sleep(3)
            data = self.api.query()
            if (data is not None) :
                logger.info(f"Received message from MSI {data}")

                # publish to all clients
                for k in self.clients:
                    q = self.clients[k][1]
                    logger.info(f"Publishing to {k} on {q}")
                    q.put(data)


    def runOrig(self) :
        self.mqttClient.run()

        while True:
            sleep(3)
            data = self.api.query()
            if (data is not None) :
                logger.info(f"Received message from MSI {data}")
                self.publish(data)


    def terminate(self) :
        for c in self.clients:
            evt = self.clients[c][2]
            logger.debug(f"{c} runflag {evt}")
            evt.clear()
            msgQ = self.clients[c][1]
            msgQ.put(None)
            thr = self.clients[c][0]
            thr.join()
            pass

def parseCmdLine(args) -> Namespace :

    parser = ArgumentParser("Collect and report MSI data to ground")

    parser.add_argument("-v", "--verbose", action="store_true",
                        help="specify output verbosity")
    parser.add_argument("-L","--logdir",
                        dest='logDir',
                        default=None,
                        help="specify log directory")

    parser.add_argument("-K,--kinesis",
                        dest="kinesisCfg",
                        default=None,
                        help="Specifies Kinesis Configuration file")

    parser.add_argument("-M,--msi",
                        dest="msiEndpoint",
                        default="https://msi.viasat.com:9100/v1/flight",
                        help="ViaSat MSI Endpoint")

    parser.add_argument("-b","--mqtt-broker",
                        dest='mqttBroker',
                        default="104.53.51.51",
                        help="MQTT Broker URL")
    parser.add_argument("-u","--user",
                        dest="userName",
                        default=os.getenv("MQTT_USER"),
                        help="MQTT User Name")

    parser.add_argument("-p","--password",
                        dest="password",
                        default=os.getenv("MQTT_PASSWORD"),
                        help="MQTT Password")

    return parser.parse_args(args)


if __name__ == "__main__" :

    params = parseCmdLine(sys.argv[1:])
    try:
        level = getattr(logging, params.logLevel.upper())
    except AttributeError as ae:
        level = logging.INFO

    except Exception as e:
        level = logging.INFO

    if params.logDir is not None:
        logfile = os.path.join(params.logDir,datetime.now().strftime("AcClient_%Y%m%d_%H%M%S.log"))
        try:
            logging.basicConfig(filename=logfile, encoding='utf-8', level=level)
        except PermissionError as pe:
            print("Unable to create log file...logging disabled to file")

    logging.basicConfig(level=level)

    logger.info("Starting Aircraft Client")
    logger.info(f"Broker: {params.mqttBroker}, User: {params.userName}")

    clientId = f"AcClient-{random.randint(1_000,10_000)}"



    svc = AircraftClient(msi=params.msiEndpoint)
    mqtt = MqttClient(params.mqttBroker, user=params.userName, passwd=params.password, clientID=clientId)
    svc.addClient("MQTT",svc.publishMqtt,mqtt)

    if params.kinesisCfg is not None:
        # Start Kinesis Client
        try:
            aws = KinesisClient(params.kinesisCfg)
            svc.addClient("Aws/Kinesis",svc.publishKinesis,aws)
        except Exception as e:
            print(f"Unable to create Kinesis Client {e}")


    try:
        mqtt.run()
        svc.run()
    except KeyboardInterrupt:
        pass
    finally:
        svc.terminate()
        mqtt.terminate()

    print("Exiting...")
