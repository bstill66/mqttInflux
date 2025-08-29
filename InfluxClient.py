import os


from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxClient(object) :
    def __init__ (self,serverUrl:str,
                  bucket : str,
                  org : str = "",
                  token:str = os.getenv("INFLUXDB_TOKEN")) :
        self.server = serverUrl
        self.bucket = bucket
        self.org    = org
        self.apiKey = token

        self.client = InfluxDBClient(url=self.server, token=self.apiKey, org=self.org)

    def createBucket(self,name:str,desc:str=None) -> None:

            bktApi = self.client.buckets_api()
            bkt = bktApi.find_bucket_by_name(name)
            if bkt is None:
                bkt = bktApi.create_bucket(bucket_name=name,
                                           description=desc,
                                           org=self.org)
            self.bucket = name

            return name is not None