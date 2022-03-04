import json
import boto3
import requests
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('yelp-restaurants')

resp = table.scan()
file_no=0
url = 'https://search-restaurants-psdxzaag6sdbtlsblkqrlunw74.us-east-1.es.amazonaws.com/res/restaurant_id'
auth = ('rohansardana', 'RohanSardana123*')
headers = {"Content-Type": "application/json"}
while True:
    print(len(resp['Items']))
    for item in resp['Items']:
        body = {"restaurantID": item['restaurantID'], "cuisine": item['cuisine']}
        print('====================UPLOADING RESTAURANT: ',body['restaurantID'], ' ====================')
        file_no +=1
        #print(body)
        r = requests.post(url,auth = auth, data=json.dumps(body).encode("utf-8"), headers=headers)
        #print(r)
        print("Adding ", file_no," file: ",r)
    if 'LastEvaluatedKey' in resp:
        resp = table.scan(
        ExclusiveStartKey=resp['LastEvaluatedKey']
        )
    else:
        break;
print(file_no," Files Added!")