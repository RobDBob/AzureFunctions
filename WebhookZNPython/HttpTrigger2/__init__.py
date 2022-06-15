import logging
import json
import azure.functions as func
from pprint import pprint

def main(req: func.HttpRequest, messageJSON) -> func.HttpResponse:

    message = json.loads(messageJSON)
    
    print("message:")
    pprint(message)
    print("messageJson:")
    pprint(messageJSON)
    
    return func.HttpResponse(f"Table row: {message}")