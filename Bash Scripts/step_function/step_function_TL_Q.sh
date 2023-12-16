#!/bin/bash
# step_function_TL_Q.sh
# Runs the TL, Q pipeline

# Define JSON payload
json_payload='{
  "s3Bucket": "python.tql.462562f23.t4.hsw",
  "s3Key": "1500000 Sales Records.csv",
  "s3db": "testdb.db"
}'

smarn="arn:aws:states:us-east-2:843281716417:stateMachine:tcss462-562-f23-TL-Q"

echo "Invoking Step function using AWS CLI"

exearn=$(aws stepfunctions start-execution --state-machine-arn $smarn --input "$json_payload" | jq -r ".executionArn")
echo "Execution ARN: $exearn"
output="RUNNING"

# Checks if the step function is still running
while [ "$output" == "RUNNING" ]
do
  echo "Status check call..."
  alloutput=$(aws stepfunctions describe-execution --execution-arn $exearn)
  output=$(echo $alloutput | jq -r ".status")
  echo "Status check=$output" # why is this null?
done

echo ""
echo "JSON RESULT:"
echo ""
aws stepfunctions describe-execution --execution-arn $exearn | jq -r ".output" | jq