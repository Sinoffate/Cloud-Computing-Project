#!/bin/bash

# Define JSON payload
json_payload=$(cat <<END
{
  "s3Bucket": "python.tql.462562f23.t4",
  "s3Key": "1500000 Sales Records.csv",
  "s3db": "TLDB.db",
  "query_params": {
    "aggregations": ["COUNT(*)"]
  }
}
END
)

# Lambda Function name and AWS region
lambda_function_name="TLQpython"
region="us-east-2" # e.g., us-east-1

# Invoke the Lambda function and capture the output
output=$(aws lambda invoke \
    --invocation-type RequestResponse \
    --function-name "$lambda_function_name" \
    --region "$region" \
    --payload "$json_payload" \
    /dev/stdout | head -n 1 | head -c -2 ; echo)

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""


