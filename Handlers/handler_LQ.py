#cloud_function(platforms=[Platform.AWS], memory=512, config=config)



def load_and_query(request, context):
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

    # Retrieve the SQLite database from S3
    s3 = boto3.client('s3')
    if 's3Bucket' in request and 's3Key' in request:
        bucket = request['s3Bucket']
        key = request['s3Key']
        db_file_path = f'/tmp/{key}'
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = pd.read_csv(response['Body'])
    else:
        logger.error('No valid data source provided.')
        inspector.addAttribute("error", "No valid data source provided.")
        return inspector.finish()

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    query_params = request['query_params']

    try:
        # Load data from the data source (e.g., a CSV file)
        df = csv_data

        # Load data into SQLite database
        conn = sqlite3.connect(db_file_path)
        df.to_sql('sales_data', conn, if_exists='replace', index=False)

        # Extract query parameters for filtering and aggregation
        filters = query_params.get('filters', [])
        aggregations = query_params.get('aggregations', [])
        group_by = query_params.get('group_by', [])

        # Construct and execute the SQL query
        select_clause = ', '.join(aggregations) if aggregations else '*'
        where_clause = ' AND '.join([f"{col}='{val}'" for col, val in filters.items()]) if filters else ''
        group_by_clause = ', '.join(group_by) if group_by else ''

        query = f"SELECT {select_clause} FROM sales_data"
        if where_clause:
            query += f" WHERE {where_clause}"
        if group_by_clause:
            query += f" GROUP BY {group_by_clause}"

        result_df = pd.read_sql_query(query, conn)
        query_results = result_df.to_dict(orient='records')

    except Exception as e:
        error_message = f"Error during load and query process: {e}"
        logger.error(error_message)
        inspector.addAttribute("error", error_message)
        return inspector.finish()
    finally:
        conn.close()

    success_message = "Data loading and querying completed successfully."
    logger.info(success_message)
    inspector.addAttribute("message", success_message)
    inspector.addAttribute("query_results", query_results)
    inspector.inspectAllDeltas()
    return inspector.finish()
