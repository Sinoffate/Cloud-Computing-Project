#!/bin/bash
# concurrent_function.sh
# To call a different script,
# edit the `output_dir`, `output_file`, `script_name` inside the loop
#Output directory and file

output_dir="./output_data/handler_T_L_Q"
output_file="$output_dir/warm_data_step_function_T_L_Q.txt"
temp_dir="$output_dir/tmp"

# Function to run the script and save output to a temporary file
run_script() {
  script_name=$1
  temp_output="$temp_dir/output_$2.txt"

  # Ensure the temporary file exists
  # touch "$temp_output"

  # Start time
  start=$(date +%s.N)

  # Run script and redirect all output (stdout and stderr) to a temporary file
  bash "$script_name" &> "$temp_output"

  # End time
  end=$(date +%s.N)

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
for i in {1..100}; do
  run_script "./step_function_T_L_Q_or_T_LQ.sh" "$i" &
done

# Wait for all background jobs to finish
wait

# Concatenate all temporary files in order and calculate the average time
for i in {1..100}; do
  cat "$temp_dir/output_$i.txt" >> "$output_file"
done

# Calculate the average time and append to the output file
awk '/Execution Time:/ {sum += $3; count++} END {print "Average Execution Time: " sum/count " seconds"}' "$output_file" >> "$output_file"

# Clean up temporary files
rm -rf "$temp_dir"