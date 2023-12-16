#cloud_function(platforms=[Platform.AWS], memory=512, config=config)
def queryDataAndStoreResults(request, context):
    import json
    import logging
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

    # Retrieve the SQLite database from S3
    s3 = boto3.client('s3')
    if 's3Bucket' in request and 's3Key' in request:
        bucket = request['s3Bucket']
        key = request['s3Key']
        db_file_path = f'/tmp/{key}'
        s3.download_file(bucket, key, db_file_path)
    else:
        logger.error('No valid data source provided.')
        inspector.addAttribute("error", "No valid data source provided.")
        return inspector.finish()

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    # Parse request to determine filters and aggregations
    query_params = request['query_params']
    filters = query_params.get('filters', [])
    aggregations = query_params.get('aggregations', [])
    group_by = query_params.get('groupBy', [])

    select_clause = ', '.join(aggregations) if aggregations else '*'
    where_clause = ' AND '.join([f"{col}='{val}'" for col, val in filters.items()]) if filters else ''
    group_by_clause = ', '.join(group_by) if group_by else ''

    # Perform SQL query
    query = f"SELECT {select_clause} FROM sales_data"
    if where_clause:
        query += f" WHERE {where_clause}"
    if group_by_clause:
        query += f" GROUP BY {group_by_clause}"

    try:
        cursor.execute(query)
        query_results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]  # Fetch column names
    except Exception as e:
        logger.error(f"Error during query execution: {e}")
        inspector.addAttribute("error", f"Error during query execution: {e}")
        conn.close()
        return inspector.finish()

    # Convert query results to JSON
    json_results = json.dumps([dict(zip(columns, row)) for row in query_results])

    # Store JSON data on S3
    try:
        json_file_key = f'{key}TLQComp.json'
        s3.put_object(Body=json_results, Bucket=bucket, Key=json_file_key)
    except Exception as e:
        logger.error(f"Error during JSON upload: {e}")
        inspector.addAttribute("error", f"Error during JSON upload: {e}")
    finally:
        conn.close()

    # Add custom message and finish the function
    inspector.addAttribute("message", "Query executed and results stored in S3")
    inspector.inspectAllDeltas()
    return inspector.finish()
