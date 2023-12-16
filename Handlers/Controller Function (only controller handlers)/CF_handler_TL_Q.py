import boto3
import json
import time

def controller_function(event, context):
    lambda_client = boto3.client('lambda')
    start_time = time.time()  # Start time for execution measurement

    # Parameters for the TL (Transform and Load) Lambda function
    params_TL = {
        "s3Bucket": "python.tql.462562f23.t4.hww",
        "s3Key": "1500000 Sales Records.csv",
        "s3db": "testdb.db"
    }

    # Parameters for the Q (Query) Lambda function
    params_Q = {
        "s3Bucket": "python.tql.462562f23.t4.hww",
        "s3Key": "testdb.db",  # Assuming this is the output from handler_TL
        "query_params": {
            "filters": {"\"Region\"": "North America", "\"Sales Channel\"": "Offline"},
            "aggregations": ["SUM(\"Total Revenue\") as TotalRevenue", "AVG(\"Unit Price\") as AveragePrice"],
            "group_by": ["\"Region\"", "\"Sales Channel\""]
        }
    }

    # Invoke the handler_TL function for data transformation and loading
    response_TL = lambda_client.invoke(
        FunctionName='handler_TL_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(params_TL)
    )
    result_TL = json.load(response_TL['Payload'])

    # Invoke the handler_Q function for data querying
    response_Q = lambda_client.invoke(
        FunctionName='handler_Q_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(params_Q)
    )
    result_Q = json.load(response_Q['Payload'])

    end_time = time.time()  # End time for execution measurement
    total_execution_time = end_time - start_time  # Calculate total execution time

    # Return the combined results along with the total execution time
    return {
        "result_TL": result_TL,
        "result_Q": result_Q,
        "total_execution_time": total_execution_time
    }

