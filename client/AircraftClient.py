import json
import os
import random
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime, timezone
from time import sleep

from ViasatMSI import ViasatMSI
from common.MqttClient import MqttClient

import logging
logger = logging.getLogger("AircraftClient")

class AircraftClient(object) :
    def __init__(self,acId:str,mqttClient:MqttClient,msi="https://msi.viasat.com:9100/v1/flight") :
        super().__init__()
        self.mqttClient = mqttClient
        self.msiUrl = msi
        self.acId = acId
        self.topic = f"Delta/{self.acId}/Viasat"
        self.api = ViasatMSI(msi)


    def publish(self,msiData) :
        header = {"tailNum" : self.acId,
                  "flightNum" : msiData["flightNumber"],
                  "timestamp" : msiData["timestamp"],
                  "flightID"  : msiData["flightId"]}

        payload = {"aircraft" : header,
                   "timestamp": str(datetime.now(timezone.utc)),
                   "data" : msiData
                   }

        payloadStr = json.dumps(payload)
        topic = msiData['vehicleId']
        del msiData['vehicleId']


        logger.info(f"Publishing to {topic}: {payloadStr}")
        self.mqttClient.publish(self.topic,payloadStr)



    def run(self) :
        self.mqttClient.run()

        while True:
            sleep(3)
            data = self.api.query()
            if (data is not None) :
                logger.info(f"Received message from MSI {data}")
                self.publish(data)


    def terminate(self) :
        pass

def parseCmdLine(args) -> Namespace :

    parser = ArgumentParser("Collect and report MSI data to ground")

    parser.add_argument("-v", "--verbose", action="store_true",
                        help="specify output verbosity")
    parser.add_argument("-L","--logdir",
                        dest='logDir',
                        default=None,
                        help="specify log directory")

    parser.add_argument("-A,--aircraft",
                        dest="acID",
                        default="N304DL",
                        help="specify Aircraft ID")

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
                        default="delta",
                        help="MQTT User Name")

    parser.add_argument("-p","--password",
                        dest="password",
                        default="KeepClimbing!",
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
        logging.basicConfig(filename=logfile, encoding='utf-8', level=level)
    else:
        logging.basicConfig(level=level)

    logger.info("Starting Aircraft Client")
    logger.info(f"Broker: {params.mqttBroker}, User: {params.userName}")

    clientId = f"{params.acID}-{random.randint(1_000,10_000)}"

    mqtt = MqttClient(params.mqttBroker,user=params.userName,passwd=params.password,clientID=clientId)

    svc = AircraftClient(acId=params.acID,mqttClient=mqtt,msi=params.msiEndpoint)

    try:
        svc.run()
    except KeyboardInterrupt:
        svc.terminate()


    mqtt.terminate()

    print("Exiting...")
