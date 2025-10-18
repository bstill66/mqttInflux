import logging
import os
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS, WriteOptions

from common.Client import Client

logger = logging.getLogger()

class InfluxClient(Client):
    def __init__ (self,serverUrl:str,
                  bucket : str,
                  org : str = "",
                  token:str = os.getenv("INFLUXDB_TOKEN")) :
        super(InfluxClient).__init__()

        self.serverUrl = serverUrl
        self.bucket = bucket
        self.server = serverUrl
        self.bucket = bucket
        self.org    = org
        self.apiKey = token

        self.client = InfluxDBClient(url=self.server, token=self.apiKey, org=self.org)
        self.writeClient = self.client.write_api(write_options=WriteOptions(batch_size=100,flush_interval=60_000),
                                                 success_callback=lambda a, b: self.writeSuccess(a, b),
                                                 error_callback=lambda a, b, c: self.writeFail(a, b, c))


    def writeSuccess(self,t,d) :
        tmp = d.split(b'\n')
        logger.info(f"Wrote {len(tmp)} records to {self.server}:{self.bucket}")


    def writeFail(self,x,y,z) :
        logger.error(f"Write failure to InfluxDB: {x},{y},{z}")



    def createBucket(self,name:str,desc:str=None) -> None:

            bktApi = self.client.buckets_api()
            bkt = bktApi.find_bucket_by_name(name)
            if bkt is None:
                bkt = bktApi.create_bucket(bucket_name=name,
                                           description=desc,
                                           org=self.org)
                if bkt is not None:
                    self.bucket = name

            return self.bucket is not None


    def write(self,ts:datetime,data:Point) -> bool :

            try:
                self.writeClient.write(bucket=self.bucket, org=self.org, record=data)
                return True
            except KeyError as ke:
                pass
            except Exception as ex:
                pass

            return False

    def terminate(self) :
        if self.writeClient is not None :
            self.writeClient.close()
        self.client.close()
