from azure.data.tables import (
    TableItem,
    TableServiceClient,
    TableClient
)
from pprint import pprint

def ReadData(rowKey):
    table_name = "outTable"
    connection_string = "DefaultEndpointsProtocol=https;AccountName=linuxfunctionuka4e0;AccountKey=3bTr603y8wob4poRqzjIyvRzU/ao9ira7hXghlan6oyeTgQd8D3WeSwshPjByOOmBO88slFZDYUD+ASt1E6nlA==;EndpointSuffix=core.windows.net"
    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = service.get_table_client(table_name)
    
    partitionKey = "message"
    pprint(table_client.get_entity(partitionKey, rowKey))

def AddData():
    table_name = "testTable"
    connection_string = "DefaultEndpointsProtocol=https;AccountName=linuxfunctionuka4e0;AccountKey=3bTr603y8wob4poRqzjIyvRzU/ao9ira7hXghlan6oyeTgQd8D3WeSwshPjByOOmBO88slFZDYUD+ASt1E6nlA==;EndpointSuffix=core.windows.net"
    
    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = service.get_table_client(table_name)

    my_entity = {
        "PartitionKey": "markers",
        "RowKey": "id-001",
        "Product": "Markers",
        "Price": 5.00,
        "Count": 10,
        "Available": True
    }

    # table_client.create_entity(my_entity)
    partitionKey = "markers"
    rowKey = "id-001"
    result = table_client.get_entity(partitionKey, rowKey)
    
    pprint(result)

if __name__ == "__main__":
    # AddData()
    ReadData("tardan")
    ReadData("stefan")