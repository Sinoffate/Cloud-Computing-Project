#cloud_function(platforms=[Platform.AWS], memory=512, config=config)
def loadData(request, context):
    import json
    import logging
    import pandas as pd
    import boto3
    import sqlite3
    from Inspector import Inspector
    import time

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    # Import the module and collect data
    inspector = Inspector()
    inspector.inspectAll()

    # Retrieve the transformed data from S3 or local storage
    s3 = boto3.client('s3')
    if 's3Bucket' in request and 's3Key' in request:
        bucket = request['s3Bucket']
        key = request['s3Key']
        db = request['s3db']
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = pd.read_csv(response['Body'])
    else:
        logger.error('No valid data source provided.')
        inspector.addAttribute("error", "No valid data source provided.")
        return inspector.finish()

    # Connect to the database
    db_filename = '/tmp/salesData.db'
    conn = sqlite3.connect(db_filename)

    # Load data into the database
    try:
        # Assuming your table is named 'Order ID'
        csv_data.to_sql('sales_data', conn, if_exists='replace', index=False)
        conn.commit()
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        inspector.addAttribute("error", f"Error during data loading: {e}")
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

    # Add custom message and finish the function
    inspector.addAttribute("message", "Data loading completed")
    inspector.inspectAllDeltas()
    return inspector.finish()
