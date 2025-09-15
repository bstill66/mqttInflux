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
logger = logging.getLogger()

class AircraftClient(object) :
    def __init__(self,mqttClient:MqttClient,msi="https://msi.viasat.com:9100/v1/flight") :
        super().__init__()
        self.mqttClient = mqttClient
        self.msiUrl = msi
        self.api = ViasatMSI(msi)


    def publish(self,msiData) :
        header = {"timestamp": str(datetime.now(timezone.utc))}

        # encode the topic for better efficiency
        topic = f"Delta/{msiData['vehicleId']}/{msiData['flightNumber']}/MSI"

        # remove since encoded in the topic
        msiData.pop('vehicleId')
        msiData.pop('flightNumber')

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
        self.mqttClient.publish(topic,payloadStr)



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

    mqtt = MqttClient(params.mqttBroker,user=params.userName,passwd=params.password,clientID=clientId)

    svc = AircraftClient(mqttClient=mqtt,msi=params.msiEndpoint)

    try:
        svc.run()
    except KeyboardInterrupt:
        pass
    finally:
        svc.terminate()
        mqtt.terminate()

    print("Exiting...")
