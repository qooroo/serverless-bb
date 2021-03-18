import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    eventType = event['requestContext']['eventType']
    connectionId = event['requestContext']['connectionId']
    connectedAt = event['requestContext']['connectedAt']

    table = boto3.resource('dynamodb', region_name='eu-west-2').Table('ws-connections')
    
    if eventType == 'CONNECT':
        table.put_item(Item={
            'connectionId': connectionId,
            'connectedAt': datetime.fromtimestamp(connectedAt/1000).isoformat()
        })
    
    elif eventType == 'DISCONNECT':
        table.delete_item(Key={'connectionId': connectionId})
    
    return {}
