import json
import boto3

def lambda_handler(event, context):
    print(event)
    lex = boto3.client('lex-runtime')
    lex_resp = lex.post_text(
        botName = 'restaurantservice',
        botAlias = 'restaurant_service_prod',
        userId = 'user01',
        inputText = event['messages'][0]['unstructured']['text'],
        activeContexts=[]
        )
    response = {
        "messages":
            [
                {"type": "unstructured",
                "unstructured":
                    {
                        "text": lex_resp['message']
                    }
                }
            ]
    }
    return response