import boto3
from datetime import datetime
import random
import secrets
import sys

def lambda_handler(event, context):
    
    for record in event['Records']:
        try:
            if record['eventName'] != 'INSERT':
                continue
            
            item = record['dynamodb']['NewImage']
            eventType = item['eventType']['S']
            
            if eventType != 'request':
                continue
            
            quoteEvent = dict(
                rfqId=item['rfqId']['S'],
                quoteId=secrets.token_hex(4),
                eventType='quote',
                timestamp=datetime.now().isoformat(),
                symbol=item['symbol']['S'],
                qty=item['qty']['S'],
                requesterSession=item['requesterSession']['S'],
                price=str(round(random.uniform(190.33, 254.66), 2)))
        
            boto3.resource('dynamodb', region_name='eu-west-2').Table('bb-rfqs').put_item(Item=quoteEvent)
            print('quote written to ddb!')
        except:
            print(f'oops:\n{sys.exc_info()}')
            
    return {}
