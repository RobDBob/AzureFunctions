import os
import ast
import logging
import json
import azure.functions as func
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime

TABLE_NAME = "outTable"
PARTITION_FIELD_VALUE = "message"
PARTITION_FIELD_NAME = "PartitionKey"
REQUIRED_PARAMETER_NAME = "entity"
DATETIME_FIELD_NAME = "DateTimeStamps"

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
    connection_string = os.environ.get("LINUXFUNCTIONUKA4E0_STORAGE")
    if not connection_string:
        connection_string = os.environ.get("AzureWebJobsStorage")

    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    return service.get_table_client(TABLE_NAME)

def POST(outputTable: func.Out[str], parameterValue):
    if not parameterValue:
        return func.HttpResponse(
             f"Parameter '{REQUIRED_PARAMETER_NAME}' not set.",
             status_code=400
        )

    currentDateTimeStamp = datetime.now().timestamp()

    tableClient = _getTableClient();
    try:
        currentEntity = tableClient.get_entity(PARTITION_FIELD_VALUE, parameterValue)
    except ResourceNotFoundError:
        currentEntity = None
        
    if currentEntity:
        if DATETIME_FIELD_NAME in currentEntity:
            dateTimeStamps = ast.literal_eval(currentEntity["DateTimeStamps"])
            dateTimeStamps.append(currentDateTimeStamp)
            currentEntity[DATETIME_FIELD_NAME] = json.dumps(dateTimeStamps)
        else:
            currentEntity[DATETIME_FIELD_NAME] = json.dumps([currentDateTimeStamp])

        tableClient.upsert_entity(currentEntity)
    else:
        data = {
            DATETIME_FIELD_NAME: [currentDateTimeStamp],
            PARTITION_FIELD_NAME: PARTITION_FIELD_VALUE, # forms the first part of an entity primary key
            "RowKey": parameterValue, # unique identifier for an entity within a given partition
        }
        outputTable.set(json.dumps(data))

    return func.HttpResponse(
        f"{REQUIRED_PARAMETER_NAME}:{parameterValue} recorded.",
        status_code=200)

def GET(parameterValue):
    """_summary_
    Retrives and deletes entity
    Args:
        req (func.HttpRequest): _description_

    Returns:
        _type_: _description_
    """    
    tableClient = _getTableClient()
    if not parameterValue:
        allEntities = tableClient.list_entities()
        allRowKeys = json.dumps([k["RowKey"] for k in allEntities])
        return func.HttpResponse(
             f"GET: all '{allRowKeys}'.",
             status_code=200
        )
    
    try:
        currentEntity = tableClient.get_entity(PARTITION_FIELD_VALUE, parameterValue)
    except ResourceNotFoundError:
        currentEntity = None
        
    if currentEntity:
        toReturn = {
            "DateTimeStamps": currentEntity.get("DateTimeStamps", [])
        }
        return func.HttpResponse(
                json.dumps(toReturn),
                status_code=200
        )
    else:
        return func.HttpResponse(f"{REQUIRED_PARAMETER_NAME} doest NOT exist: {parameterValue}", status_code=404)
    
def DELETE(parameterValue):
    tableClient = _getTableClient()

    if not parameterValue:
        allEntities = tableClient.list_entities()
        allRowKeys = [k["RowKey"] for k in allEntities]
        for rowKey in allRowKeys:
            tableClient.delete_entity(PARTITION_FIELD_VALUE, rowKey)
        return func.HttpResponse(
            f"Removed all: {json.dumps(allRowKeys)}.",
            status_code=200
    )

    tableClient.delete_entity(PARTITION_FIELD_VALUE, parameterValue)

    return func.HttpResponse(
            f"{REQUIRED_PARAMETER_NAME} removed '{REQUIRED_PARAMETER_NAME}:{parameterValue}'.",
            status_code=200
    )


def main(req: func.HttpRequest, outputTable: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    parameterValue = _getParameterOut(req, REQUIRED_PARAMETER_NAME)
    
    if req.method == "POST":
        return POST(outputTable, parameterValue)
    elif req.method == "GET":
        return GET(parameterValue)
    elif req.method == "DELETE":
        return DELETE(parameterValue)
    else:
        return func.HttpResponse(
             "Supported methods are POST & GET.",
             status_code=200
        )
