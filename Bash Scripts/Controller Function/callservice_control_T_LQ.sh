#!/bin/bash

# Lambda Function name and AWS region
lambda_function_name="control_T_LQ"
region="us-east-2"

# Output directory and file
output_dir="./lambda_results"
output_file="$output_dir/results_control_T_LQ.txt"
temp_dir="$output_dir/temp"

# Function to invoke Lambda function and save output to a temporary file
invoke_lambda() {
    local index=$1
    local temp_output="$temp_dir/output_$index.txt"

    # Start time
    start=$(date +%s.%N)

    # Invoke the Lambda function with an increased timeout and redirect output to a temporary file
    output=$(aws lambda invoke \
        --invocation-type RequestResponse \
        --function-name "$lambda_function_name" \
        --region "$region" \
        --cli-read-timeout 120 \
        /dev/stdout | head -n 1 | head -c -2 ; echo)

    echo "$output" &> "$temp_output"

    # End time and execution time calculation
    end=$(date +%s.%N)
    runtime=$(echo "$end - $start" | bc)
    echo "Execution Time: $runtime seconds" >> "$temp_output"
}

# Ensure the output and temporary directories exist
mkdir -p "$output_dir"
mkdir -p "$temp_dir"

# Clear the output file if it already exists
> "$output_file"

# Running Lambda function 100 times concurrently
for i in {1..100}; do
    invoke_lambda $i &
done

# Wait for all background jobs to finish
wait

# Concatenate all temporary files in order and calculate the average time
for i in {1..100}; do
    cat "$temp_dir/output_$i.txt" >> "$output_file"
done

# Calculate the average time and append to the output file
awk '/Execution Time:/ {sum += $3; count++} END {print "Average Execution Time: " sum/count " seconds"}' "$output_file" >> "$output_file"

# Print average execution time to the console
average_time=$(awk '/Average Execution Time:/ {print $4 " " $5}' "$output_file")
echo "Average Total Execution Time: $average_time"

# Clean up temporary files
rm -rf "$temp_dir"


