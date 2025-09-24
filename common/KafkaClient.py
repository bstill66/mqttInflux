import json
from email.policy import default

from confluent_kafka import Producer, Consumer

def read_config(cfgPath:str):
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open(cfgPath) as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce(topic, config):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  key = "key"
  value = "value"
  producer.produce(topic, key=key, value=value)
  print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

  # send any outstanding or buffered messages to the Kafka broker
  cnt = producer.flush()
  print(f"{cnt} messages flushed")

def consume(topic, config):
  # sets the consumer group ID and offset
  config["group.id"] = "python-group-1"
  config["auto.offset.reset"] = "earliest"

  # creates a new consumer instance
  consumer = Consumer(config)

  # subscribes to the specified topic
  consumer.subscribe([topic])
  consumer.list_topics(topic)

  try:
    while True:
      # consumer polls the topic and prints any incoming messages
      msg = consumer.poll(1.0)
      if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
  except KeyboardInterrupt:
    pass
  finally:
    # closes the consumer connection
    consumer.close()

def main():
    configPath = "./common/client.properties"
    config = read_config(configPath)
    topic = "brian"

    produce(topic, config)
    consume(topic, config)


def onOff(v:str) -> str:
    return "Ground" if v == "On" else "Air"




KAFKA_XFORM = {
    "flightId": (None,None,None),
    "timestamp" : (None,None,None),
    "flightOriginDate": (None,None,None),
    "estimatedArrivalTime": (None,None,None),
    "elapsedFlightTimeMinutes": (None,None,None),
    "operatingCarrierCode":(None,"DL",None),
    "flightNumber": (None,None,None),
    "latitude": (None,None,None),
    "longitude":(None,None,None),
    "ship":("noseId",None,None),
    "paState":(None,None,None),
    "registrationNumber":(None,None,None),
    "destinationCode":("destination",None,None),
    "originCode":("origin",None,None),
    "currentAirspeedKts":(None,None,None),
    "currentAirTempuratureCelsius":(None,None,None),
    "altitudeFeet":("altitude",None,None),
    "nauticalMilesRemaining":None,
    "doorState":None,
    "currentGroundSpeedKts":("groundspeed",None,None),
    "currentHeading":None,
    "flightTimeRemainingMinutes":None,
    "wheelsOnGround":("wheelWeightState",None,lambda x: onOff(x)),
    "aircraftGrossWeight":None
}

def transform(src: object) -> object:
    # transform the MSI information into a Kafka object
    kafka = {}
    for k in KAFKA_XFORM.keys():
        fld = None
        dflt = None
        func = None

        try:
            fld = KAFKA_XFORM[k][0]
            dflt = KAFKA_XFORM[k][1]
            func = KAFKA_XFORM[k][2]
        except TypeError:
           pass

        try:
            if fld is not None:
                srcVal = src[fld]
            else:
                srcVal = src[k]
            if func is not None:
                dstVal = func(srcVal)
            else:
                dstVal = srcVal

            kafka[k] = dstVal
        except TypeError as e:
            kafka[k] = None
        except Exception as e:
            kafka[k] = dflt

    return kafka



def testXform() :
    msi = {
    "timestamp": "2025-09-18T19:59:53Z",
    "latitude": 33.6566162109375,
    "longitude": -84.42444610595703,
    "noseId": "4002",
    "destination": "KATL",
    "origin": "KBOS",
    "flightId": "TESTDL08_SF_20250918172803",
    "altitude": 10000,
    "doorState": "Closed",
    "groundspeed": 200,
    "heading": -177,
    "wheelWeightState": "On",
    "flightPhase": "Arrival"}

    kfka = transform(msi)

    assert(kfka['flightId'] == msi['flightId'])
    assert(kfka['timestamp'] == msi['timestamp'])
    # assert(kfka['flightOriginDate'] == msi['flightOriginDate'])
    assert(kfka['estimatedArrivalTime'] == None)
    assert(kfka['elapsedFlightTimeMinutes'] == None)
    assert(kfka['operatingCarrierCode'] == "DL")
    #assert(kfka['flightNumber'] == msi['flightNumber'])
    assert(kfka['latitude'] == msi['latitude'])
    assert(kfka['longitude'] == msi['longitude'])
    assert(kfka['ship'] == msi['noseId'])
    assert(kfka['paState'] == None)
    assert(kfka['destinationCode'] == msi['destination'])
    assert(kfka['originCode'] == msi['origin'])
    assert(kfka['currentAirspeedKts'] == None)
    assert(kfka['currentAirTempuratureCelsius'] == None)
    assert(kfka['altitudeFeet'] == msi['altitude'])
    assert(kfka['nauticalMilesRemaining'] == None)
    assert(kfka['doorState'] == "Closed")
    assert(kfka['currentGroundSpeedKts'] == msi['groundspeed'])
    assert(kfka['currentHeading'] is None)
    assert(kfka['flightTimeRemainingMinutes'] == None)
    assert(kfka['wheelsOnGround'] == "Ground")
    assert(kfka['aircraftGrossWeight'] == None)


if __name__ == "__main__":
    testXform()

    main()


