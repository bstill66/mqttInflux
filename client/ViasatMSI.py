import json
import logging
import sys
from datetime import datetime, timezone

from time import sleep
import requests



class ViasatMSI(object) :
    def __init__(self,url="https://msi.viasat.com:9100/v1/flight") :
        super().__init__()
        self.url = url

    def query(self) -> dict:

        try:
            if False:

                    rsp = requests.get(self.url,timeout=2.5,
                                       headers={'Accept': 'application/json'})

            else:
                rsp = type("", (dict,), {"status_code": 200, "text": ""})()
                rsp.text = '{ "timestamp": "' + f"{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}" + '",'

                rsp.text += """
                "airspeed" : 410.0,
                "altitude": 33000.0,
                "groundspeed": 420.0,
                "eta" : "02:30",
                "heading": 180.0,
                "latitude": 30.4469485748939,
                "longitude": -99.234234
                }"""

            if rsp.status_code == 200:

                jsonRsp = json.loads(rsp.text)

                return jsonRsp
            else:
                logging.error(f"Unable to query MSI: {rsp.status_code}")
        except Exception as e :
            pass

        return None



ENDPOINT = "https://msi.viasat.com:9100/v1/flight"

if __name__ == "__main__" :

    if len(sys.argv) > 1 :
        ENDPOINT = sys.argv[1]


    api = ViasatMSI(url=ENDPOINT)

    for i in range(10) :
        rsp = api.query()
        if (rsp is None) :
            print("Failed to get response")

        sleep(1)


    print("Done")
