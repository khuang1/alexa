import boto3
import json
import random
import time
import datetime
import httplib
import urllib
import logging
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

print('Loading function')

def lambda_handler(event, context):
    '''Provide an event that contains the following keys:
      - operation: one of the operations in the operations dict below
      - tableName: required for operations that interact with DynamoDB
      - payload: a parameter to pass to the operation being performed
    '''
    #print("Received event: " + json.dumps(event, indent=2))
    #print("Log stream name:", context.log_stream_name)
    #print("Log group name:", context.log_group_name)
    #print("Request ID:", context.aws_request_id)
    #print("Mem. limits(MB):", context.memory_limit_in_mb)
    #print("Time remaining (MS):", context.get_remaining_time_in_millis())

    operation = event['operation']
    clientVal = event['payload']

    dynamo = boto3.resource('dynamodb').Table('EchoUsers')
    dynamo.load()

    curt  = datetime.datetime.utcnow()
    epoch = datetime.datetime(1970, 1, 1)
    secs  = int((curt - epoch).total_seconds())
    print(secs)

    operations = {
        'placeCallTest': lambda x: dynamo.scan(),
        'placeCall': lambda x: dynamo.scan(FilterExpression=Attr('Lasttime').lt(str(secs-30))),
        'echo': lambda x: x
    }

    if operation in operations:
        response =  operations[operation](event['payload'])
        #if operation == 'placeCall' or operation == 'placeCallTest':
        if operation == 'placeCall':
            totalCount = response['Count']
            if totalCount > 0:
                selitem = (response['Items'][random.randint(0, totalCount-1)])
                accessToken = ""
                dynamo.update_item (
                    Key={'Phone': selitem['Phone']},
                    AttributeUpdates={
                        'Lasttime': {
                            'Value': str(secs),
                            'Action': 'PUT'
                        },
                        'From': {
                            'Value': clientVal['from'],
                            'Action': 'PUT'
                        },
                        'To': {
                            'Value': clientVal['to'],
                            'Action': 'PUT'
                        }
                    },
                    Expected={ 'Phone': {'Value': selitem['Phone']}}
                )
                #Call API Gateway to get tokens
                #Host in dodo environment
                serv = "api.dodo.vocal-dev.com"
                #Host in production environment
                #serv = "api.vonagebusiness.com"
                if selitem['Reftoken'] == 'na' or selitem['Acctoken'] == 'na' or secs > int(selitem['Expire'])-30:
                    #connection in dodo environment
                    conn = httplib.HTTPConnection(serv)
                    #connection in production environment
                    #conn = httplib.HTTPSConnection(serv)
                    head = {"Content-type": "application/x-www-form-urlencoded", "Authorization": "Basic bW9iaWxlOg==", "Accept": "application/json"}
                    body = "grant_type=password&username="+selitem['User']+"&password="+selitem['Password']+"&client_id=mobile"
                    conn.request("POST", "/oauth/token", body, head)
                    resp = conn.getresponse()
                    data = resp.read()
                    conn.close()
                    #print(data)
                    if resp.status == 200:
                        resj = json.loads(data)
                        accessToken = resj['access_token']
                        dynamo.update_item (
                            Key={'Phone': selitem['Phone']},
                            AttributeUpdates={
                                'Acctoken': {
                                    'Value': accessToken,
                                    'Action': 'PUT'
                                },
                                'Reftoken': {
                                    'Value': resj['refresh_token'],
                                    'Action': 'PUT'
                                },
                                'Expire': {
                                    'Value': str(secs + int(resj['expires_in'])),
                                    'Action': 'PUT'
                                }
                            },
                            Expected={ 'Phone': {'Value': selitem['Phone']}}
                        )
                    else:
                        logger.error('Service error when calling API Gateway to get tokens.')
                        raise ValueError('Service error in Vonage Echo System - token error "{}"'.format(operation))
                else:
                    accessToken = selitem['Acctoken']
                #print("accessToken = " + accessToken)
                #Setting up call forwarding
                #connection in dodo environment
                conn = httplib.HTTPConnection(serv)
                #connection in production environment
                #conn = httplib.HTTPSConnection(serv)
                accessToken = "bearer " + accessToken
                head = {"Content-type": "application/json", "Authorization": accessToken}
                body = "{\"basicInfo\": {\"userName\": \""+selitem['User']+"\", \"firstName\": \""+selitem['Firstname']+"\", \"lastName\": \""+selitem['Lastname']+"\", \"userId\": 0, \"accountId\": \""+selitem['Account']+"\"}, \"extensions\": [{\"extension\": \""+selitem['Extension']+"\", \"didList\": [\""+selitem['Phone']+"\"], \"nmac\": {\"choice\": \"forwardAllCalls\", \"settings\": {\"forwardAllCalls\": {\"phoneNumber\": \""+clientVal['to']+"\"}, \"callerIdOnForwardedCallId\": \"\"}}}]}"
                #print(body)
                conn.request("PUT", "/hdap/adminv2/api/ucaas/user/settings", body, head)
                resp = conn.getresponse()
                #print("SETTING resp STATUS: "+str(resp.status))
                #data = resp.read()
                conn.close()
                #print(data)
                #Make click2callme call
                if resp.status == 200:
                    #time.sleep( 1 )
                    #connection in dodo environment
                    conn = httplib.HTTPConnection(serv)
                    #connection in production environment
                    #conn = httplib.HTTPSConnection(serv)
                    comm = "/hdap/appserver/rest/click2callme/"+selitem['Didkey']+"/?phonenumber="+clientVal['from']+"&callback=doResponse&nocache=5&"
                    conn.request("GET", comm, "", head)
                    resp = conn.getresponse()
                    conn.close()
                    if resp.status == 200:
                        return (clientVal)
                    else:
                        logger.error('Service error - Click2CallMe failed in Vonage Echo System')
                        raise ValueError('Service error - Click2CallMe failed in Vonage Echo System "{}"'.format(operation))
                else:
                    logger.error('Service error in Vonage Echo System')
                    raise ValueError('Service error in Vonage Echo System - call failed "{}"'.format(operation))
            else:
                logger.error('No service slot is available in Vonage Echo System')
                raise ValueError('No service slot is available in Vonage Echo System "{}"'.format(operation))
        else:
            logger.info('Un-supported operation in Vonage Echo System')
            return (response)
    else:
        logger.error('Unrecognized operation')
        raise ValueError('Unrecognized operation "{}"'.format(operation))

