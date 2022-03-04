import boto3
import json
import datetime
from decimal import *

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('yelp-restaurants')

filenames = ["indian_restaurant.json", "chinese_restaurant.json", "mexican_restaurant.json", "italian_restaurant.json", "japanese_restaurant.json", "korean_restaurant.json"]
cuisines = ["indian", "chinese", "mexican", "italian", "japanese", "korean"]

for f,element in enumerate(filenames):
    filename = element
    file_no=0
    print('====================UPLOADING FILE: ',filename, ' ====================')
    with open(filename) as json_file:
        Restaurants = json.load(json_file)
        #print(len(Restaurants)) = 20
        #print(len(Restaurants[0]['businesses']))
        for ele in range(len(Restaurants)):
                if(len(Restaurants[ele]['businesses']) > 0):
                    #print(len(Restaurants[ele]['businesses']))
                    for i in range(len(Restaurants[ele]['businesses'])):
                        file_no+=1
                        #print(len(Restaurants[ele]['businesses']))
                        business = Restaurants[ele]['businesses'][i]
                        for restaurant in business:
                            restaurantID = business['id']
                            alias = business['alias']
                            name = business['name']
                            rating = Decimal(business['rating'])
                            numReviews = business['review_count']
                            address = business['location']['display_address']
                            latitude = Decimal(str(business['coordinates']['latitude']))
                            longitude = Decimal(str(business['coordinates']['longitude']))
                            zip_code = business['location']['zip_code']
                            cuisine = cuisines[f]
                            city = business['location']['city']
                            phone = business['phone']
                        
                        
                
                        print("Adding ",file_no," ",filename," Restaurant: ",restaurantID, alias, name, rating, numReviews, address, latitude, longitude, zip_code, cuisine, city, phone)
                
                        table.put_item(
                          Item={
                              'insertedAtTimestamp': str(datetime.datetime.now()),
                              'restaurantID': restaurantID,
                              'alias': alias,
                              'name': name,
                              'rating': rating,
                              'numReviews': numReviews,
                              'address': address,
                              'latitude': latitude,
                              'longitude': longitude,
                              'zip_code': zip_code,
                              'cuisine': cuisine,
                              'city': city,
                              'phone': phone,
                            }
                        )