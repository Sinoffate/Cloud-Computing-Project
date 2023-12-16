#cloud_function(platforms=[Platform.AWS], memory=512, config=config)
from io import StringIO


def transformData(request, context):
    import json
    import logging
    import pandas as pd
    import boto3
    from Inspector import Inspector
    import time

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    # Import the module and collect data
    inspector = Inspector()
    inspector.inspectAll()

    lambda_client = boto3.client('lambda')

    # Determine CSV data source (either from request or S3)
    if 's3Bucket' in request and 's3Key' in request:
        s3 = boto3.client('s3')
        bucket = request['s3Bucket']
        key = request['s3Key']
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = pd.read_csv(response['Body'])
    else:
        logger.error('No valid data source provided.')
        inspector.addAttribute("error", "No valid data source provided.")
        return inspector.finish()

    # Perform data transformations
    try:
        # Add Order Processing Time column
        csv_data['Order Processing Time'] = (pd.to_datetime(csv_data['Ship Date']) -
                                             pd.to_datetime(csv_data['Order Date'])).dt.days

        # Transform Order Priority column
        priority_map = {'L': 'Low', 'M': 'Medium', 'H': 'High', 'C': 'Critical'}
        csv_data['Order Priority'] = csv_data['Order Priority'].map(priority_map)

        # Add Gross Margin column
        csv_data['Gross Margin'] = csv_data['Total Profit'] / csv_data['Total Revenue']

        # Remove duplicates
        csv_data.drop_duplicates(subset='Order ID', inplace=True)

        # Store data in S3
        transformed_key = 'transformed_' + key
        csv_buffer = StringIO()
        csv_data.to_csv(csv_buffer)
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=transformed_key)

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        return inspector.finish()

    next_payload = {
        's3Bucket': bucket,
        's3Key': transformed_key,
        'query_params': {
            'aggregations': ["COUNT(*)"]

        }
    }

    response = lambda_client.invoke(
        FunctionName='handler_L_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(next_payload)
    )

    # Add custom message and finish the function
    inspector.addAttribute("message", "Data transformation completed")
    inspector.inspectAllDeltas()
    return inspector.finish()
