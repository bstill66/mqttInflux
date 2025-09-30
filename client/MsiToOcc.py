import re


def onOff(v:str) -> str:
    return "Ground" if v == "On" else "Air"

FLTNUM_RE = re.compile("(?P<CC>\\D+)(?P<NUM>\\d+)")
def fltNum(fn:str) -> int :
    m = FLTNUM_RE.match(fn)
    if m is not None:
        return int(m.group("NUM"))
    return None


KAFKA_XFORM = {
    "flightId": (None,None,None),
    "timestamp" : (None,None,None),
    "flightOriginDate": (None,None,None),
    "estimatedArrivalTime": (None,None,None),
    "elapsedFlightTimeMinutes": (None,None,None),
    "operatingCarrierCode":(None,"DL",None),
    "flightNumber": (None,None,fltNum),
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




