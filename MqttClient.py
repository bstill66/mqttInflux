import logging
import sys
from time import sleep, time
from typing import Callable

from paho.mqtt.client import Client
from paho.mqtt.enums import CallbackAPIVersion


class MqttClient(object) :
    FIRST_RECONNECT_DELAY = 1
    RECONNECT_RATE = 2
    MAX_RECONNECT_COUNT = 12
    MAX_RECONNECT_DELAY = 60


    def __init__(self, server: str = "localhost", port: int = 1883) :
        # keep the server and port information
        self.server = server
        self.port = port

        # create the client instance (that does most of the work)
        self.mqClient = Client(callback_api_version=CallbackAPIVersion.VERSION2,
                               userdata=self)

        # setup the callbacks
        self.mqClient.on_connect    = self._onConnect
        self.mqClient.on_disconnect = self._onDisconnect
        self.mqClient.on_message    = self._onMessage

        self.subMsgCount = 0
        self.pubMsgCount = 0
        self.abort = False
        self.topics = {}

    def onConnect(self,client:Client,flags,rc,prop) :
        logging.info("Connected with result code "+str(rc))

    @staticmethod
    def _onConnect(client, ud, flags, rc, prop) :
        if hasattr(ud,'onConnect') :
            ud.onConnect(client,flags,rc,prop)




    @staticmethod
    def _onDisconnect(client, ud, flags, rc, p):
        if hasattr(ud, 'onDisconnect') :
            ud.onDisconnect(client,flags,rc,p)

    def onDisconnect(self,client,flags,rc,prop):
        if self.abort:
            return

        logging.info("Disconnected with result code: %s", rc)

        reconnect_count = 0
        reconnect_delay = 0
        while reconnect_count < self.MAX_RECONNECT_COUNT:
            logging.info("Reconnecting in %d seconds...", reconnect_delay)
            sleep(reconnect_delay)

            try:
                client.reconnect()
                logging.info("Reconnected successfully!")
                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= self.RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, self.MAX_RECONNECT_DELAY)
            reconnect_count += 1

        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)


    def onMessage(self,client:Client,msg) :
        self.msgCount += 1
        logging.info("Message received: " + str(msg.payload))

    @staticmethod
    def _onMessage(client, ud, msg):
        if hasattr(ud,'onMessage') :
            ud.onMessage(client,msg)

    def onMessage(self,client:Client,msg) :
        self.subMsgCount += 1
        logging.info("Message received: " + str(msg.payload))

        if msg.topic in self.topics:
            usrFunc,usrData = self.topics[msg.topic]
            if usrFunc is not None:
                usrFunc(msg,usrData)


    def doConnect(self,srvr:str,port:int) -> bool :
        if self.mqClient.connect(self.server, self.port, 60) != 0:
            logging.warning("Couldn't connect to the mqtt broker")
            return False
        return True

    def initialize(self) :
        pass

    def subscribe(self,topic:str,cb:Callable[[str],None],usrParam) -> None :
        if topic not in self.topics :
            logging.info(f"Adding topic {topic}")
            res = self.mqClient.subscribe(topic)
        self.topics[topic] = (cb,usrParam)

    def publish(self,topic:str,msg,qos:int=0,retain:bool=False) :
        self.pubMsgCount += 1
        self.mqClient.publish(topic,msg,qos,retain)

    def run(self):

        self.initialize()

        connected = False
        while not self.abort and not connected :
            sleep(self.FIRST_RECONNECT_DELAY)
            connected = self.doConnect(self.server,self.port)

        self.mqClient.loop_start()


    def terminate(self):
        self.abort = True
        self.mqClient.disconnect()






if __name__ == "__main__" :
    SERVER = "localhost"

    if len(sys.argv) > 1:
        SERVER = sys.argv[1]

    sub = MqttClient("localhost")

    sub.run()
    sub.subscribe("flight/info", None,None)

    pub = MqttClient("localhost")
    pub.run()


    for i in range(1,20) :
        x = pub.publish("flight/info", f"{i}", 2,retain=True)

    sleep(3)


    assert(sub.subMsgCount >= pub.pubMsgCount);

    pub.terminate()


    sub.terminate()


