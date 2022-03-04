import json
import boto3
import random
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import warnings
from elasticsearch.exceptions import ElasticsearchWarning
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
message = ''

# host = "search-restaurants-psdxzaag6sdbtlsblkqrlunw74.us-east-1.es.amazonaws.com"
endpoint = 'https://search-restaurants-psdxzaag6sdbtlsblkqrlunw74.us-east-1.es.amazonaws.com'
credentials = boto3.Session().get_credentials()
aws_session_token = credentials.token
headers = { "Content-Type": "application/json" }
awsauth=AWS4Auth(credentials.access_key,credentials.secret_key, 'us-east-1', 'es', session_token = aws_session_token)

# index = "elastic-search"

# url = endpoint + '/' + index + '/_search'


def lambda_handler(event, context):
    
    warnings.simplefilter('ignore', ElasticsearchWarning)

    sqs = boto3.client('sqs')
    dynamodb = boto3.resource('dynamodb')
    queue_url = "https://sqs.us-east-1.amazonaws.com/220343102758/Q1"

    # polling messaging from sqs
    response = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0)
    
    print("This is the response from sqs ->",response)
    #print("This is the event from sqs ->",event)

    cuisine = response['Messages'][0]['MessageAttributes']['Cuisine']['StringValue']
    #print(cuisine)
    #extracting other things from sqs
    phoneNumber = response['Messages'][0]['MessageAttributes']['PhoneNumber']['StringValue']
    email = response['Messages'][0]['MessageAttributes']['EmailAddress']['StringValue']
    location=response['Messages'][0]['MessageAttributes']['Location']['StringValue']
    numOfPeople=response['Messages'][0]['MessageAttributes']['CountPeople']['StringValue']
    date=response['Messages'][0]['MessageAttributes']['DiningDate']['StringValue']
    time=response['Messages'][0]['MessageAttributes']['DiningTime']['StringValue']
    print(cuisine, phoneNumber, email, location, numOfPeople, date, time)
    if not cuisine or not email:
        logger.debug("No Cuisine or Email key found in message!")
        return
    
    
    # sqs2 = boto3.client('sqs')
    # queue_url2 = "https://sqs.us-east-1.amazonaws.com/220343102758/Q1"
    
    # reciept_handle = response['Messages'][0]['ReceiptHandle']
    # sqs2.delete_message(QueueUrl = queue_url2, ReceiptHandle = reciept_handle )
    
    
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
    # extract bID from AWS ES
    
    ids = []
    for restaurant in esData:
        ids.append(restaurant["_source"]["restaurantID"])
    print(ids)
    messageToSend = 'Hello! Here are my {cuisine} restaurant suggestions in {location} for {numPeople} people, for {diningDate} at {diningTime}: '.format(
            cuisine=cuisine,
            location=location,
            numPeople=numOfPeople,
            diningDate=date,
            diningTime=time,
        )
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    itr = 1
    prevRestaurants=""
    Restaurants=[]
    for id in ids:
        if itr == 6:
            break
        print(id)
        response = table.get_item(Key={'restaurantID': {'S': id}})
        print(response)
        if response is None or "Item" not in response.keys():
            continue
        print(response)
        item = response['Item']
        restaurantMsg = '' + str(itr) + '. '
        name = item["name"]
        print(name)
        prevRestaurants+=str(name)+","
        address = item["address"]
        print(address)
        
        restaurantMsg += name +', located at ' + address[0] +'. '
        messageToSend += restaurantMsg
        print(messageToSend)
        
        itr += 1
    if prevRestaurants:
        response = table.put_item(
                Item={
                                'restaurantID':"prevRestaurants" ,
                                'name':prevRestaurants[:-1],
                })
    messageToSend += "Enjoy your meal!!"
    print(messageToSend)
    print(phoneNumber)