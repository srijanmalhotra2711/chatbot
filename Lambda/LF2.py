import json
import boto3
import random
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import os
import warnings
from elasticsearch.exceptions import ElasticsearchWarning
from boto3.dynamodb.conditions import Key, Attr
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

endpoint = 'https://search-restaurants-psdxzaag6sdbtlsblkqrlunw74.us-east-1.es.amazonaws.com'
credentials = boto3.Session().get_credentials()
aws_session_token = credentials.token
headers = { "Content-Type": "application/json" }
#awsauth1=AWS4Auth(credentials.access_key,credentials.secret_key, 'us-east-1', 'es', session_token = aws_session_token)
awsauth = (os.environ['es_user'], os.environ['es_pass'])

sender_email_addr = "rohan.sardana.nyu@gmail.com"
message = ''

def send_email_using_ses(messageToSend, email, cuisine, location):
    ses = boto3.client('ses')
    verifiedResponse = ses.list_verified_email_addresses()
    if email not in verifiedResponse['VerifiedEmailAddresses']:
        verifyEmailResponse = ses.verify_email_identity(EmailAddress=email)
        return
    # Send a mail to the user regarding the restaurant suggestions
    mailResponse = ses.send_email(
        Source=sender_email_addr,
        Destination={'ToAddresses': [email]},
        Message={
            'Subject': {
                'Data': "Dining Conceirge Chatbot has a message for you!",
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': messageToSend,
                    'Charset': 'UTF-8'
                }
            }
        }
    )

def lambda_handler(event, context):
    
    warnings.simplefilter('ignore', ElasticsearchWarning)

    sqs = boto3.client('sqs')
    dynamodb = boto3.resource('dynamodb')
    queue_url = "https://sqs.us-east-1.amazonaws.com/220343102758/Q1"

    #polling messages from sqs
    response = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0)
    cuisine = response['Messages'][0]['MessageAttributes']['Cuisine']['StringValue']
    
    #extracting other things from sqs
    phoneNumber = response['Messages'][0]['MessageAttributes']['PhoneNumber']['StringValue']
    email = response['Messages'][0]['MessageAttributes']['EmailAddress']['StringValue']
    location=response['Messages'][0]['MessageAttributes']['Location']['StringValue']
    numOfPeople=response['Messages'][0]['MessageAttributes']['CountPeople']['StringValue']
    date=response['Messages'][0]['MessageAttributes']['DiningDate']['StringValue']
    time=response['Messages'][0]['MessageAttributes']['DiningTime']['StringValue']
    if not cuisine or not email:
        logger.debug("No Cuisine or Email key found in message!")
        return
    
    
    sqs2 = boto3.client('sqs')
    queue_url2 = "https://sqs.us-east-1.amazonaws.com/220343102758/Q1"
    reciept_handle = response['Messages'][0]['ReceiptHandle']
    sqs2.delete_message(QueueUrl = queue_url2, ReceiptHandle = reciept_handle )
    
    # The OpenSearch domain endpoint with https://
    index = 'res'
    url = endpoint + '/' + index + '/_search'
    print(url)
    query = {
        "query": {
        "match": {
        "cuisine": cuisine 
        }
    }
    }
    
    
    # Elasticsearch 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    # Make the signed HTTP request
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    data = json.loads(r.content.decode('utf-8'))
    
    print("datadatadatadata",data)
    esData=[]
    
    try:
        esData = data["hits"]["hits"]
    except KeyError:
        logger.debug("Error extracting hits from ES response")
    print(esData)
    # extract restaurantID from AWS ES
    
    ids = []
    for restaurant in esData:
        ids.append(restaurant["_source"]["restaurantID"])
        
    messageToSend = 'Hey there!\n\n\nHere are my {cuisine} restaurant suggestions in {location} for {numPeople} people for {diningDate} at {diningTime}:\n\n'.format(
            cuisine=cuisine.capitalize(),
            location=location.capitalize(),
            numPeople=numOfPeople,
            diningDate=date,
            diningTime=time,
        )
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    dynamodb_client = boto3.client('dynamodb', region_name="us-east-1")
    itr = 1
    Restaurants=[]
    for id in ids:
        if itr == 6:
            break
        # response = table.get_item(Key={'restaurantID': id})
        response = dynamodb_client.query(
    TableName='yelp-restaurants',
    KeyConditionExpression='restaurantID = :restaurantID',
    ExpressionAttributeValues={
        ':restaurantID': {'S': id}
    })
        if response is None or "Items" not in response.keys():
            
            continue
        item = response['Items'][0]
        restaurantMsg = '' + str(itr) + '. '
        name = item["name"]["S"]
        address = item["address"]["L"]
        rating = item["rating"]["N"]
        phone = item["phone"]["S"]
        
        restaurantMsg += 'Name: '+name +'\nAddress: ' + address[0]["S"] +'\n'+ address[1]["S"]+ '\nRating: '+ rating + '\nContact No.: '+ phone + '\n'
        messageToSend += '\n'+restaurantMsg
        itr += 1
    messageToSend += "\n\nEnjoy your meal!!\nYour Dining Chatbot Concierge  :)"
    print(messageToSend)
    print(phoneNumber)
    
    send_email_using_ses(messageToSend, email, cuisine, location)