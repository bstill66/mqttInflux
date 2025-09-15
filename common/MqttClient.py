
import os
import random
import re
import sys
from time import sleep, time
from typing import Callable

from paho.mqtt.client import Client
from paho.mqtt.enums import CallbackAPIVersion

import logging

logger = logging.getLogger("")

class MqttClient(object) :
    FIRST_RECONNECT_DELAY = 1
    RECONNECT_RATE = 2
    MAX_RECONNECT_COUNT = 12
    MAX_RECONNECT_DELAY = 60


    def __init__(self, server: str = "localhost", port: int = 1883,user:str=None,passwd:str=None,clientID:str=None,) :
        # keep the server and port information
        self.server = server
        self.port = port

        # create the client instance (that does most of the work)
        self.mqClient = Client(callback_api_version=CallbackAPIVersion.VERSION2,
                               userdata=self,
                               clean_session=False,
                               client_id=clientID)

        if (user is not None) :
            self.mqClient.username_pw_set(user,passwd)
            logger.info(f"Connecting to {self.server} as {user}")

        # setup the callbacks
        self.mqClient.on_connect    = self._onConnect
        self.mqClient.on_disconnect = self._onDisconnect
        self.mqClient.on_message    = self._onMessage
        self.mqClient.on_subscribe  = self._onSubscribe

        self.subMsgCount = 0
        self.pubMsgCount = 0
        self.subRxCount  = 0
        self.abort = False
        self.topics = {}

    def onConnect(self,client:Client,flags,rc,prop) :
        logger.info("Connected with result code "+str(rc))

    @staticmethod
    def _onConnect(client, ud, flags, rc, prop) :
        logger.info(f"Connected to MQTT Broker @ {ud.server}:{ud.port}")
        if hasattr(ud,'onConnect') :
            ud.onConnect(client,flags,rc,prop)




    @staticmethod
    def _onDisconnect(client, ud, flags, rc, p):
        logger.info(f"Disconnecting from {ud.server}")
        if hasattr(ud, 'onDisconnect') :
            ud.onDisconnect(client,flags,rc,p)

    def onDisconnect(self,client,flags,rc,prop):
        if self.abort:
            return

        logger.info("Disconnected with result code: %s", rc)

        reconnect_count = 0
        reconnect_delay = 0
        while reconnect_count < self.MAX_RECONNECT_COUNT:
            logger.info("Reconnecting in %d seconds...", reconnect_delay)
            sleep(reconnect_delay)

            try:
                client.reconnect()
                logger.info("Reconnected successfully!")
                return
            except Exception as err:
                logger.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= self.RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, self.MAX_RECONNECT_DELAY)
            reconnect_count += 1

        logger.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)


    @staticmethod
    def _onMessage(client, ud, msg):
        if hasattr(ud,'onMessage') :
            ud.onMessage(client,msg)

    def onMessage(self,client:Client,msg) :
        self.subRxCount += 1
        logger.info(f"Message received: " + str(msg.payload))

        for k in self.topics:
            pat,usrFunc,usrData = self.topics[k]
            m = pat.match(msg.topic)
            if m is not None:
                if usrFunc is not None:
                    usrFunc(msg, usrData)
                    self.subMsgCount += 1


    def doConnect(self,srvr:str,port:int) -> bool :
        if self.mqClient.connect(self.server, self.port, 60) != 0:
            logger.warning("Couldn't connect to the mqtt broker")
            return False
        return True

    def initialize(self) :
        pass

    def _onSubscribe(self,client,ud,flags,rc,p) :
        if hasattr(ud,'onSubscribe') :
            ud.onSubscribe(client,flags,rc,p)


    def cvtToRegEx(self,topic:str) -> re.Pattern :
        tmp = topic
        tmp = tmp.replace('#',".*")
        tmp = tmp.replace('+',".*")

        return re.compile(tmp)

    def subscribe(self,topic:str,cb:Callable[[str],None],usrParam) -> None :

        if topic not in self.topics :
            logging.info(f"Adding topic {topic}")
            res = self.mqClient.subscribe(topic)
        self.topics[topic] = (self.cvtToRegEx(topic),cb,usrParam)

    def publish(self,topic:str,msg,qos:int=0,retain:bool=False) :
        self.pubMsgCount += 1
        self.mqClient.publish(topic,msg,qos,retain)

    def run(self):

        self.initialize()

        connected = False
        while not self.abort and not connected :
            sleep(self.FIRST_RECONNECT_DELAY)
            try:
                connected = self.doConnect(self.server,self.port)
            except OSError as err:
                connected = False

        self.mqClient.loop_start()


    def terminate(self):
        self.abort = True
        self.mqClient.disconnect()






if __name__ == "__main__" :
    SERVER = "104.53.51.51"
    USER   = os.getenv("MQTT_USER")
    PWD    = os.getenv("MQTT_PASSWORD")

    if len(sys.argv) > 1:
        SERVER = sys.argv[1]


    sub = MqttClient(SERVER,user=USER,passwd=PWD,
                     clientID=f"sub-{random.uniform(10_000,100_000)}")

    sub.run()


    count1 = 0
    count2 = 0

    def incCount1() :
        global count1
        count1 += 1

    def incCount2() :
        global count2
        count2 += 1

    sub.subscribe("flight/+/status", lambda m,d: incCount1() ,None)
    sub.subscribe("delta/flight/#",lambda m,d: incCount2(),None)

    pub = MqttClient(SERVER,user=USER,passwd=PWD,
                     clientID=f"pub-{random.uniform(10_000,100_000)}")
    pub.run()

    pub1 = 0
    pub2 = 0
    for i in range(0,20) :
        x = pub.publish("flight/DL77/status", f"{i}", 2,retain=True)
        pub1 += 1

        x = pub.publish(f"delta/flight/{i}",f"{i}",2,retain=True)
        pub2 += 1

        sleep(1)



    assert(count1 >= pub1);
    assert(count2 >= pub2);


    pub.terminate()
    sub.terminate()
    print("Success!")


