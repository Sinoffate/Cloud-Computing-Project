import boto3
import json
import time

def controller_function(event, context):
    lambda_client = boto3.client('lambda')
    start_time = time.time()

    # Parameters for each Lambda function
    params_T = {
        "s3Bucket": "python.tql.462562f23.t4.hww",
        "s3Key": "1500000 Sales Records.csv"
    }

    params_L = {
        "s3Bucket": "python.tql.462562f23.t4.hww",
        "s3Key": "1500000 Sales Records.csv",
        "s3db": "testdb.db"
    }

    params_Q = {
        "s3Bucket": "python.tql.462562f23.t4.hww",
        "s3Key": "testdb.db",
        "query_params": {
            "filters": {
                "\"Region\"": "North America",
                "\"Sales Channel\"": "Offline"
            },
            "aggregations": ["SUM(\"Total Revenue\") as TotalRevenue", "AVG(\"Unit Price\") as AveragePrice"],
            "group_by": ["\"Region\"", "\"Sales Channel\""]
        }
    }

    # Invoke handler_T
    response_T = lambda_client.invoke(
        FunctionName='handler_T_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(params_T)
    )
    result_T = json.load(response_T['Payload'])

    # Invoke handler_L
    response_L = lambda_client.invoke(
        FunctionName='handler_L_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(params_L)
    )
    result_L = json.load(response_L['Payload'])

    # Invoke handler_Q
    response_Q = lambda_client.invoke(
        FunctionName='handler_Q_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(params_Q)
    )
    result_Q = json.load(response_Q['Payload'])
    
    end_time = time.time()  # End time
    total_execution_time = end_time - start_time  # Total execution time

    # Return the combined result
    return {
        "result_T": result_T,
        "result_L": result_L,
        "result_Q": result_Q,
        "total_execution_time": total_execution_time
    }




