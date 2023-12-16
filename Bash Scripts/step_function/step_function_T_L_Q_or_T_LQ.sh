#!/bin/bash
# step_function_T_L_Q_or_T_LQ.sh
# Invokes T_L_Q pipeline and T_LQ pipeline

# Change the `smarn` to invoke between the two pipelines from above

# Define JSON payload
json_payload='{
  "s3Bucket": "python.tql.462562f23.t4.hw",
  "s3Key": "1500000 Sales Records.csv"
}'

smarn="arn:aws:states:us-east-2:843281716417:stateMachine:tcss462-562-f23-T-L-Q"

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
  echo "Status check=$output" 
done

echo ""
echo "JSON RESULT:"
echo ""
aws stepfunctions describe-execution --execution-arn $exearn | jq -r ".output" | jq



