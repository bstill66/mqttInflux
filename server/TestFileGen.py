import json
import logging
import re
from datetime import datetime, timezone
from multiprocessing import Event
from time import sleep

from common.MqttClient import MqttClient

logger = logging.getLogger()

MSI_SAMPLE = '''
{"timestamp":"2025-10-16T12:08:46Z","eta":"02:35","flightDuration":12,"flightNumber":"DAL1516","latitude":37.22229967994918,"longitude":-112.15674058996935,"noseId":"004001","paState":true,"vehicleId":"TESTDL15","destination":"KCVG","origin":"KATL","flightId":"TESTDL15_SF_20251014183628","airspeed":410,"airTemperature":32,"altitude":33000,"distanceToGo":999,"doorState":"Closed","groundspeed":420,"heading":180,"timeToGo":12,"wheelWeightState":"Off","grossWeight":500,"windSpeed":150,"windDirection":200.0,"flightPhase":"Cruise"}
'''

class TestFileGen(object) :
    SAMPLE_RE = re.compile(".*TOPIC:[(?P<TOPIC>.*)] PAYLOAD:[(?P<PAYLOAD.*)]")
    TOPIC_RE  = re.compile("Delta/(?P<TAIL>[a-zA-z]{1}\\d{3}[a-zA-z]{2})/(?P<FLIGHT>[a-zA-z]{3}\\d+)/MSI")

    def __init__(self,client:MqttClient,fnames) :
        self.mqtt = client
        self.finished = Event()
        self.filenames = fnames

    def cvtToPublisher(self,data:str) -> str:
        header = {"timestamp": str(datetime.now(timezone.utc))}

        if isinstance(data,str) :
            data = json.loads(data)

        # encode the topic for better efficiency
        topic = f"Delta/{data['vehicleId']}/{data['flightNumber']}/MSI"

        # remove since encoded in the topic
        # msiData.pop('vehicleId')
        # msiData.pop('flightNumber')

        # Don't send empty data elements
        dataToSend = {}
        for k in data.keys():
            if data[k] != None:
                dataToSend[k] = data[k]

        payload = {"header": header,
                   "data": dataToSend
                   }

        payloadStr = json.dumps(payload)
        return payloadStr


    def run(self) :
        self.mqtt.run()

        try:
            for fname in self.filenames :
                with open(fname) as f :
                    for line in f:
                        m = self.SAMPLE_RE.match(line)
                        if m is not None:
                            logger.info(f"{m.group('TOPIC')}:{m.group('PAYLOAD')}")
                            payload = m.group("PAYLOAD")
                        payload = self.cvtToPublisher(MSI_SAMPLE)
                        topic   = "Delta/NXXXUS/DAL345/MSI"
                        sleep(2)

                        self.mqtt.publish(topic,payload)
        except FileNotFoundError :
            logger.warning("File {fname} not found")

        self.finished.set()



    def terminate(self) :
        pass



    def waitToComplete(self) :
        self.finished.wait()

