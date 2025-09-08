import json

import requests

DEV_URL = "https://msi.viasat.com:9100/v1/devices"
if __name__ == "__main__" :

    for i in range(0,256) :
        addr = f"172.19.0.{i}"

        try:
            url = f"{DEV_URL}/{addr}"
            rsp = requests.get(url, timeout=2.5,
                               headers={'Accept': 'application/json'})

            if rsp.status_code == 200 :
                res = json.loads(rsp.text)


        except requests.exceptions.RequestException :
            pass

