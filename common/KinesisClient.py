import json
import logging
import os

import boto3
import botocore

from common.Client import Client

logger = logging.getLogger()


def strcmp(str1, str2):
    if len(str1) != len(str2) :
        return False

    for i in range(0,len(str1)) :
        if str1[i] != str2[i] :
            return False

    return True

class KinesisClient(Client) :
    def __init__(self,cfg:str=None):
        super(KinesisClient, self).__init__(cfg)

        self.client = boto3.client('kinesis',
                                   aws_access_key_id=self.config['aws_access_key_id'],
                                   aws_secret_access_key=self.config['aws_secret_access_key'],
                                   region_name=self.config['region_name'])



    def publish(self,stream:str,data) :
        if isinstance(data,str) :
            data = json.loads(data)
        if stream is None:
            stream = self.config['default_stream']
        try:
            logger.debug("putting record to kinesis")
            rsp = self.client.put_record(StreamName=stream,
                                         Data=json.dumps(data),
                                         PartitionKey="parition-1")
            logger.debug("finished putting record to kinesis")
            return rsp
        except botocore.exceptions.SSLError as e:
            logger.warning("Problem connecting to Kinesis Server")
        except Exception as e:
            logger.exception(e)

        return None

    def terminate(self) :
        self.client.close()


if __name__ == "__main__" :
    TEST_MSI = """
    """
    pub  = KinesisClient("config/DalKinesis.cfg")

    rsp = pub.publish("telemetryDataStream",'{"test" : "data"}')
    print(rsp)


