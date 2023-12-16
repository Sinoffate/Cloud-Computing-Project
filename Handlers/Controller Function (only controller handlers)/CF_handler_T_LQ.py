import boto3
import json
import time

def controller_function(event, context):
    lambda_client = boto3.client('lambda')
    start_time = time.time()  # Start time for execution measurement

    # Parameters for the Transform (T) Lambda function
    params_T = {
        "s3Bucket": "python.tql.462562f23.t4.hww",
        "s3Key": "1500000 Sales Records.csv"
    }

    # Parameters for the Load and Query (LQ) Lambda function
    params_LQ = {
        "s3Bucket": "python.tql.462562f23.t4.hww",
        "s3Key": "transformed_1500000 Sales Records.csv",  # Assuming this is the output of handler_T
        "query_params": {
            "filters": {
                "\"Region\"": "North America",
                "\"Sales Channel\"": "Offline"
            },
            "aggregations": ["SUM(\"Total Revenue\") as TotalRevenue", "AVG(\"Unit Price\") as AveragePrice"],
            "group_by": ["\"Region\"", "\"Sales Channel\""]
        }
    }

    # Invoke handler_T for data transformation
    response_T = lambda_client.invoke(
        FunctionName='handler_T_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(params_T)
    )
    result_T = json.load(response_T['Payload'])

    # Invoke handler_LQ for data loading and querying
    response_LQ = lambda_client.invoke(
        FunctionName='handler_LQ_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(params_LQ)
    )
    result_LQ = json.load(response_LQ['Payload'])

    end_time = time.time()  # End time for execution measurement
    total_execution_time = end_time - start_time  # Calculate total execution time

    # Return the combined result along with the total execution time
    return {
        "result_T": result_T,
        "result_LQ": result_LQ,
        "total_execution_time": total_execution_time
    }

