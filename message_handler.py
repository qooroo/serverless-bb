import boto3
import json
from datetime import datetime
import secrets
import sys

def lambda_handler(event, context):
    msg = event['body'].split()
    table = boto3.resource('dynamodb', region_name='eu-west-2').Table('bb-rfqs')
    
    if msg[0] == 'request':
        rfqEvent = dict(
            rfqId=secrets.token_hex(4),
            eventType='request',
            timestamp=datetime.now().isoformat(),
            symbol=msg[1],
            qty=msg[2],
            requesterSession=event['requestContext']['connectionId'])
        ack = dict(msg='REQUEST ACCEPTED', rfqId=rfqEvent['rfqId'])
    elif msg[0] == 'trade':
        try:
            rfqEvents = table.query(KeyConditionExpression='rfqId = :id', ExpressionAttributeValues={':id': msg[1]})
        except:
            print(f'oops, error retrieving:\n{sys.exc_info()}')

        rfqEvent = next((x for x in rfqEvents['Items'] if x['eventType'] == 'quote'), None)
        rfqEvent['eventType']='trade'
        rfqEvent['timestamp']=datetime.now().isoformat()
        ack = dict(msg='TRADE ACCEPTED', rfqId=rfqEvent['rfqId'])

    table.put_item(Item=rfqEvent)

    return {
        'statusCode': 200,
        'body': json.dumps(ack)
    }
