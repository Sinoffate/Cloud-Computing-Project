#cloud_function(platforms=[Platform.AWS], memory=512, config=config)


def transform_and_load(request, context):
    import json
    import logging
    import pandas as pd
    import boto3
    import sqlite3
    from Inspector import Inspector
    import time

    # Initialize logging and inspector
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    inspector = Inspector()
    inspector.inspectAll()

    lambda_client = boto3.client('lambda')

    s3 = boto3.client('s3')
    if 's3Bucket' in request and 's3Key' in request:
        bucket = request['s3Bucket']
        key = request['s3Key']
        db = request['s3db']
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
    else:
        logger.error('No valid data source provided.')
        inspector.addAttribute("error", "No valid data source provided.")
        return inspector.finish()

    try:

        # Perform necessary transformations
        # Add Order Processing Time column
        df['Order Date'] = pd.to_datetime(df['Order Date'])
        df['Ship Date'] = pd.to_datetime(df['Ship Date'])
        df['Order Processing Time'] = (df['Ship Date'] - df['Order Date']).dt.days

        # Transform Order Priority column
        priority_map = {'L': 'Low', 'M': 'Medium', 'H': 'High', 'C': 'Critical'}
        df['Order Priority'] = df['Order Priority'].map(priority_map)

        # Calculate Gross Margin column
        df['Gross Margin'] = df['Total Profit'] / df['Total Revenue']

        # Remove duplicates
        df.drop_duplicates(subset='Order ID')

        # Load data into SQLite database
        db_filename = '/tmp/salesData.db'
        conn = sqlite3.connect(db_filename)
        df.to_sql('sales_data', conn, if_exists='replace', index=False)

    except Exception as e:
        error_message = f"Error during transform and load process: {e}"
        logger.error(error_message)
        inspector.addAttribute("error", error_message)
        return inspector.finish()
    finally:
        conn.close()


    # Upload the SQLite DB to S3
    try:
        with open(db_filename, 'rb') as db_file:
            s3.put_object(Body=db_file, Bucket=bucket, Key=db)
    except Exception as e:
        logger.error(f"Error during database upload: {e}")
        inspector.addAttribute("error", f"Error during database upload: {e}")
        return inspector.finish()
    finally:
        conn.close()

    # Json payload for the next lambda function
    next_payload = {
        's3Bucket': bucket,
        's3Key': db,
        'query_params': {
            'aggregations': ["COUNT(*)"]

        }
    }
    response = lambda_client.invoke(
        FunctionName='handler_Q_hww',
        InvocationType='RequestResponse',
        Payload=json.dumps(next_payload)
    )
    success_message = "Data transformation and loading completed successfully."
    logger.info(success_message)
    inspector.addAttribute("message", success_message)
    inspector.inspectAllDeltas()
    return inspector.finish()
