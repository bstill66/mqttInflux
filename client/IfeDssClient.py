import json
import logging
import os
import random
import sys
from argparse import Namespace, ArgumentParser
from datetime import datetime, timezone, timedelta
from multiprocessing import Queue, Event
from queue import Empty
from threading import Thread
from time import sleep
from typing import Callable

import requests

from client.ViasatMSI import ViasatMSI
from common.MqttClient import MqttClient
from DssSim import DssSimulator
logger = logging.getLogger()


class IfeDssClient(object) :
    AUTH_KEY = 'Basic UW9FOlhUdUk5dFduUWRkZWtmdA=='

    def __init__(self,
                 qoeUrl="https://smartife-local-api.delta.com:50802/qoe/snapshot",
                 msiApi="https://msi.viasat.com:9100/v1/flight",
                 msiUpdateRateSec: int = 60) :
        super().__init__()

        self.qoeUrl = qoeUrl
        self.msiUrl = msiApi
        self.msiUpdateRate = timedelta(seconds=msiUpdateRateSec)
        self.lastUpdate = datetime.min

        self.startFlag = Event()
        self.clients = {}

        self.msiUrl = "http://192.168.1.129:9100/v1/flight"


    def addClient(self,name:str,func:Callable,client) :
        q = Queue()
        evt = Event()
        evt.set()

        thr = Thread(target=self.clientRun,args=(name,q,evt,func,client))
        self.clients[name] = (thr,q,evt,func,client)
        thr.start()


    def publishMqtt(self,data,mqtt) :
        if mqtt is None or not isinstance(mqtt,MqttClient) :
            return

        header = {"timestamp": str(datetime.now(timezone.utc))}


        # Don't send empty data elements
        dataToSend = {}
        for k in data.keys() :
            if data[k] != None :
                dataToSend[k] = data[k]

        payload = {"header" : header,
                   "data" : dataToSend
                   }

        payloadStr = json.dumps(payload)


        logger.info(f"Publishing to {self.topic}, timestamp: {data['timestamp']}")
        logger.info(f"TOPIC:[{self.topic}] PAYLOAD:[{payloadStr}]")
        mqtt.publish(self.topic,payloadStr)


    def preProcess(self,data:dict) -> [dict,None]:
        pub = {}
        dss = {}
        seats = data['Seats']
        for s in seats:
            for fld in seats[s]:
                if fld == "UI":
                    fld = f"UI_{seats[s][fld]}"
                    val = 1
                else:
                    val = seats[s][fld]

                if (val == 1) :
                    try:
                        pub[fld] += 1
                    except KeyError:
                        pub[fld] = 1

        if len(pub) > 0:
            pub['timestamp'] =  str(data['timestamp'])
        else:
            pub = None
        return pub

    def clientRun(self,name:str,msgQ:Queue,runFlag:Event,publish:Callable,client) -> None:
        logger.info(f"Client {name} waiting to start")
        self.startFlag.wait()
        logger.info(f"Client {name} started on {msgQ}")

        while runFlag.is_set() :

            try:
                data = msgQ.get(timeout=3)
                if data is not None:
                    logger.info(f"Client {name} publishing {data['timestamp']}")

                    pubData = self.preProcess(data)
                    if pubData is not None:
                        publish(pubData,client)
                else:
                    pass
            except Empty:
                pass
            except Exception as e:
                logger.exception(e)
                pass

        logger.info(f"Client {name} exiting")


    def query(self) :
        try:
            if self.sim is not None:
                return self.sim.get()
            rsp = requests.get(self.qoeUrl,
                               timeout=30,
                               headers={'Accept': 'application/json',
                                        'Content-Type': 'application/json',
                                        'Authorization': self.AUTH_KEY})

            if rsp.status_code in (200,) :
                payload = json.loads(rsp.text)
                logger.info(f"{payload}")

                return payload
            elif rsp.status_code in (401,) :
                logger.info(f"Authorization failure: {self.AUTH_KEY}")
            else:
                logger.info(f"Received status : {rsp.status_code}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"encountered Connection error: {self.qoeUrl}")
        except Exception as e:
            s = type(e)
            logger.exception(e)

        return None

    def run(self) :
        self.startFlag.set()

        while True:
            timeSinceLastUpdate = datetime.now() - self.lastUpdate
            if timeSinceLastUpdate > (3 * self.msiUpdateRate):
                msiQry = ViasatMSI(self.msiUrl)
                try:
                    rsp = msiQry.query()

                    if rsp is not None:
                        self.tailNum = rsp['vehicleId']
                        self.flightNum = rsp['flightNumber']
                        self.topic = f"Delta/{self.tailNum}/{self.flightNum}/DSS"
                        self.lastUpdate = datetime.now()
                    else:
                        sleep(3)

                except Exception as e:
                    sleep(3)
                continue

            data = self.query()
            if (data is not None) :
                data['timestamp'] = datetime.now(timezone.utc)
                logger.info(f"Received message from DSS {data['timestamp']}")

                # publish to all clients
                for k in self.clients:
                    q = self.clients[k][1]
                    logger.info(f"Publishing to {k} on {q}")
                    q.put(data)

            sleep(15)



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

    parser = ArgumentParser("Collect and report DSS data to ground")

    parser.add_argument("-v", "--verbose", action="store_true",
                        help="specify output verbosity")

    parser.add_argument("-L","--logdir",
                        dest='logDir',
                        default=None,
                        help="specify log directory")

    parser.add_argument("-e","--endpoint",
                             dest='endpoint',
                             default='https://smartife-local-api.delta.com:50802/qoe/snapshot',
                             help="Specify endpoint")

    parser.add_argument("-b","--mqtt-broker",
                        dest='mqttBroker',
                        default=os.getenv("MQTT_URL","104.53.51.51"),
                        help="MQTT Broker URL")
    parser.add_argument("-u","--user",
                        dest="userName",
                        default=os.getenv("MQTT_USER","delta"),
                        help="MQTT User Name")

    parser.add_argument("-p","--password",
                        dest="password",
                        default=os.getenv("MQTT_PASSWORD"),
                        help="MQTT Password")

    return parser.parse_args(args)


if __name__ == "__main__" :

    if False:
        import http.client as http_client
        # Enable debugging for http.client (for raw request/response data)
        http_client.HTTPConnection.debuglevel = 1

        # Configure basic logging for the root logger
        logging.basicConfig(level=logging.DEBUG)

        # Get the logger for 'requests.packages.urllib3' and set its level to DEBUG
        # This logger handles the lower-level HTTP details
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True  # Ensure messages are passed to the root logger

        # Optional: Get the logger for 'requests' and set its level (usually INFO or DEBUG)
        # This logger handles higher-level requests information
        requests_main_log = logging.getLogger("requests")
        requests_main_log.setLevel(logging.DEBUG)

    params = parseCmdLine(sys.argv[1:])
    try:
        level = getattr(logging, params.logLevel.upper())
    except AttributeError as ae:
        level = logging.INFO

    except Exception as e:
        level = logging.INFO

    if params.logDir is not None:
        logfile = os.path.join(params.logDir,datetime.now().strftime("DssIfeClient_%Y%m%d_%H%M%S.log"))
        try:
            logging.basicConfig(filename=logfile, encoding='utf-8', level=level)
        except PermissionError as pe:
            print("Unable to create log file...logging disabled to file")

    logging.basicConfig(level=level)

    logger.info("Starting DssIfe Client")
    logger.info(f"Broker: {params.mqttBroker}, User: {params.userName}")

    clientId = f"DssIfeClient-{random.randint(1_000,10_000)}"



    svc = IfeDssClient(params.endpoint)
    svc.sim = DssSimulator()

    mqtt = MqttClient(params.mqttBroker, user=params.userName, passwd=params.password, clientID=clientId)
    svc.addClient("MQTT",svc.publishMqtt,mqtt)



    try:
        mqtt.run()
        svc.run()
    except KeyboardInterrupt:
        pass
    finally:
        svc.terminate()
        mqtt.terminate()

    print("Exiting...")
