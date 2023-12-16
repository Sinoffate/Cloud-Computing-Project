#!/bin/bash

# Output directory and file
output_dir="./lambda_data/lambda_TLQ"
output_file="$output_dir/cold_data_TLQ.txt"
temp_dir="$output_dir/temp"

# Function to run your script and save output to a temporary file
run_script() {
    script_name=$1
    temp_output="$temp_dir/output_$2.txt"

    # Start time
    start=$(date +%s.%N)

    # Run script and redirect all output (stdout and stderr) to a temporary file
    bash "$script_name" &> "$temp_output"

    # End time
    end=$(date +%s.%N)

    # Execution time
    runtime=$(echo "$end - $start" | bc)

    # Append the execution time to the temporary file
    echo "Execution Time: $runtime seconds" >> "$temp_output"
}

# Ensure the output and temporary directories exist
mkdir -p "$output_dir"
mkdir -p "$temp_dir"

# Clear the output file if it already exists
> "$output_file"

# Running script 100 times concurrently
for i in {1..100}
do
    run_script "./callservice_q.sh" "$i" &
done

# Wait for all background jobs to finish
wait

# Concatenate all temporary files in order and calculate the average time
for i in {1..100}
do
    cat "$temp_dir/output_$i.txt" >> "$output_file"
done

# Calculate the average time and append to the output file
awk '/Execution Time:/ {sum += $3; count++} END {print "Average Execution Time: " sum/count " seconds"}' "$output_file" >> "$output_file"


# Clean up temporary files
rm -rf "$temp_dir"