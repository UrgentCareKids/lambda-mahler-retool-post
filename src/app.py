import json

def handler(event, context):
    body = event['Records'][0]['Sns']['Message']
    body = json.loads(body)
    operation = body['operation']
    print(operation)
