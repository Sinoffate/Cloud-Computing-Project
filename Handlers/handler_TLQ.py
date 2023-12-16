import json
import logging
import boto3
import pandas as pd
import sqlite3
from Inspector import Inspector


def extract_and_transform(data_source):
    # Read CSV data
    df = data_source

    # Transformation 1: Calculate Order Processing Time
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])
    df['Order Processing Time'] = (df['Ship Date'] - df['Order Date']).dt.days

    # Transformation 2: Transform Order Priority
    priority_map = {'L': 'Low', 'M': 'Medium', 'H': 'High', 'C': 'Critical'}
    df['Order Priority'] = df['Order Priority'].map(priority_map)

    # Transformation 3: Calculate Gross Margin
    df['Gross Margin'] = df['Total Profit'] / df['Total Revenue']

    # Transformation 4: Remove duplicates
    df.drop_duplicates(subset='Order ID', inplace=True)
    return df


def load_data(transformed_data):
    db_path = '/tmp/databaseTLQ.db'

    # Create a connection to the SQLite database
    conn = sqlite3.connect(db_path)

    try:
        # Write the data to a sqlite table
        transformed_data.to_sql('sales_data', conn, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error during loading data into the database: {e}")
        conn.close()
        raise

    return conn


def query_data(db_connection, query_params):
    # Extract parameters
    filters = query_params.get('filters', [])
    aggregations = query_params.get('aggregations', [])
    group_by = query_params.get('group_by', [])

    # Construct the SQL query
    select_clause = ', '.join(aggregations) if aggregations else '*'
    where_clause = ' AND '.join([f"{col}='{val}'" for col, val in filters.items()]) if filters else ''
    group_by_clause = ', '.join(group_by) if group_by else ''

    query = f"SELECT {select_clause} FROM sales_data"
    if where_clause:
        query += f" WHERE {where_clause}"
    if group_by_clause:
        query += f" GROUP BY {group_by_clause}"

    # Execute the query
    try:
        result_df = pd.read_sql_query(query, db_connection)
        return result_df.to_dict(orient='records')
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"Error during query execution: {e}")
        raise


# Main Lambda handler
def lambda_handler(request, context):
    inspector = Inspector()
    inspector.inspectAll()
    logging.info(f"Request: {request}")

    if 's3Bucket' in request and 's3Key' in request:
        s3 = boto3.client('s3')
        bucket = request['s3Bucket']
        key = request['s3Key']
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data = pd.read_csv(response['Body'])
    else:
        logging.error('No valid data source provided.')
        inspector.addAttribute("error", "No valid data source provided.")
        return inspector.finish()

    try:
        # Extract and Transform
        transformed_data = extract_and_transform(csv_data)
    except Exception as e:
        logging.error(f"Error during extract and transform: {e}")
        inspector.addAttribute("error", f"Error during extract and transform: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    try:
        # Load
        db_conn = load_data(transformed_data)
    except Exception as e:
        logging.error(f"Error during load: {e}")
        inspector.addAttribute("error", f"Error during load: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    try:
        # Query
        query_results = query_data(db_conn, request['query_params'])

        # Process and return results
        json_results = json.dumps(query_results)
        return {
            'statusCode': 200,
            'body': json_results
        }
    except Exception as e:
        logging.error(f"Error during query: {e}")
        inspector.addAttribute("error", f"Error during query: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        inspector.addAttribute("message", "TLQ pipeline executed")
        inspector.inspectAllDeltas()
        inspector.finish()
