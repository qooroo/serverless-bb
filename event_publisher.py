import boto3
import json
import sys

publisher = None
ws_table = None

def lambda_handler(event, context):
    
    global publisher
    if not publisher:
        publisher = boto3.client('apigatewaymanagementapi', endpoint_url='TODO')
        
    global ws_table
    if not ws_table:
        ws_table = boto3.resource('dynamodb', region_name='eu-west-2').Table('ws-connections')
        
    for record in event['Records']:
        try:
            if record['eventName'] != 'INSERT':
                continue
            
            item = record['dynamodb']['NewImage']
            eventType = item['eventType']['S']
            
            event = dict(
                rfqId=item['rfqId']['S'],
                quoteId=item['quoteId']['S'],
                eventType=eventType,
                timestamp=item['timestamp']['S'],
                symbol=item['symbol']['S'],
                qty=item['qty']['S'],
                price=item['price']['S'],
                requesterSession=item['requesterSession']['S'])
                    
            if eventType == 'quote':
                publisher.post_to_connection(Data=json.dumps(event, indent=2).encode(), ConnectionId=item['requesterSession']['S'])
            elif eventType == 'trade':
                for conn in ws_table.scan()['Items']:
                    try:
                        publisher.post_to_connection(Data=json.dumps(event, indent=2).encode(), ConnectionId=conn['connectionId'])
                    except:
                        print('¯\_(ツ)_/¯')
        except:
            print(f'oops: {sys.exc_info()}')
            
    return {}
