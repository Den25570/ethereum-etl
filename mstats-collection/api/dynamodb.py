import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
import datetime

my_config = Config(
    region_name = 'us-east-2',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)

class DynamoDbConnection:

    dynamodb: boto3.resource

    def __init__(self):
        session = boto3.Session(profile_name='default')
        self.dynamodb = session.resource('dynamodb', config=my_config)
        

    def uploadBlocks(self, blocks):
        table = self.dynamodb.Table('Blocks')
        for batchIndex in range(0, len(blocks), 25):
            with table.batch_writer() as batch:
                for index in range(batchIndex, min(batchIndex + 25, len(blocks))):
                    batch.put_item(blocks[index])

    def queryBlocksByDate(self, date):
        table = self.dynamodb.Table('Blocks')
        response = table.query(
            KeyConditionExpression=Key('date').eq(date)
        )
        return response['Items']

    def queryBlocksByInterval(self, startDate, endDate, interval):

        keys=[]
        keyDate = startDate
        while keyDate < endDate:  
            keys.append(
                {
                'date': datetime.datetime.fromtimestamp(keyDate).strftime("%Y-%m-%d"), 
                'hour': int(datetime.datetime.fromtimestamp(keyDate).strftime("%H"))
                })
            keyDate += interval

        response = dynamodb.batch_get_item(
            RequestItems={
                'Blocks': {
                    'Keys': keys
                }
            }
        )

        return response['Responses']['Blocks']

    def getLatestTimestamp(self, date, blockNumber):
        table = self.dynamodb.Table('Blocks')
        response = table.query(
              Limit = 1,
              ScanIndexForward = False,
              KeyConditionExpression=Key('date').eq(date)
           )
        return response