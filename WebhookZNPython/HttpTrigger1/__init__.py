import os
import logging
import json
import azure.functions as func

from azure.data.tables import (
    TableItem,
    TableServiceClient,
    TableClient
)

from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ServiceRequestError,
    ResourceNotFoundError,
    AzureError
)

# stuff to do:
# 1. get & delete
# set env variables to work for both local & deployed

from pprint import pprint


tableName = "outTable"
partitionKey = "message"
parameterName = "name"

def _getParameterOut(req, parameterName):
    parameterValue = req.params.get(parameterName)
    if not parameterValue:
        try:
            req_body = req.get_json()
        except ValueError:
            parameterValue = None
        else:
            parameterValue = req_body.get(parameterName)
    return parameterValue

def _getTableClient():
    connection_string = os.environ["LINUXFUNCTIONUKA4E0_STORAGE"]
    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    return service.get_table_client(tableName)
    

def POST(req: func.HttpRequest, outputTable: func.Out[str]):
    parameterValue = _getParameterOut(req, parameterName)

    data = {
        "Data": f"Output binding message: {parameterValue}",
        "PartitionKey": "message", # forms the first part of an entity primary key
        "RowKey": parameterValue, # unique identifier for an entity within a given partition
    }
    
    try:
        currentEntity = _getTableClient().get_entity(partitionKey, parameterValue)
    except ResourceNotFoundError:
        currentEntity = None

    if currentEntity:
        # entity already exists
        # return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
        return func.HttpResponse(f"POST: entity already exists: {parameterValue}", status_code=409)
    else:
        outputTable.set(json.dumps(data))
        return func.HttpResponse(
             "POST: This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

def GET(req: func.HttpRequest):
    """_summary_
    Retrives and deletes entity
    Args:
        req (func.HttpRequest): _description_

    Returns:
        _type_: _description_
    """
    parameterValue = _getParameterOut(req, parameterName)
    
    if not parameterValue:
        return func.HttpResponse(
             f"GET: parameter '{parameterName}' not set.",
             status_code=400
        )
    
    tableClient = _getTableClient()
    
    try:
        currentEntity = tableClient.get_entity(partitionKey, parameterValue)
    except ResourceNotFoundError:
        currentEntity = None
        
    if currentEntity:
        # tableClient.delete_entity(partitionKey, parameterValue)

        return func.HttpResponse(
                json.dumps(currentEntity),
                status_code=200
        )
    else:
        return func.HttpResponse(f"GET: entity doest NOT exist: {parameterValue}", status_code=404)
    
def DELETE(req: func.HttpRequest):
    parameterValue = _getParameterOut(req, parameterName)
    
    if not parameterValue:
        return func.HttpResponse(
             f"DELETE: parameter '{parameterName}' not set.",
             status_code=400
        )
    
    tableClient = _getTableClient()
    
    tableClient.delete_entity(partitionKey, parameterValue)

    return func.HttpResponse(
            f"DELETE: entity removed '{parameterName}'.",
            status_code=200
    )


def main(req: func.HttpRequest, outputTable: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if req.method == "POST":
        return POST(req, outputTable)
    elif req.method == "GET":
        return GET(req)
    elif req.method == "DELETE":
        return DELETE(req)
    else:
        return func.HttpResponse(
             "Supported methods are POST & GET.",
             status_code=200
        )

