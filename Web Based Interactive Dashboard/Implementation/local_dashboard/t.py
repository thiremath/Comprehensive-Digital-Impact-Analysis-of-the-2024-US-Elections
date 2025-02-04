import pandas as pd

# Define the input and output file names
input_file = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/reddit_data_copy.csv"
filtered_file = "filtered_data.csv"
daily_submissions_file = "daily_submissions.csv"

# Load the data, skipping problematic rows
try:
    df = pd.read_csv(input_file, header=None, on_bad_lines='skip')  # Skips problematic lines
except Exception as e:
    print(f"Error reading the file: {e}")
    raise

# Convert the timestamp column (assuming column 5 is the timestamp column)
df[5] = pd.to_datetime(df[5], errors='coerce')

# Define the date range
start_date = pd.Timestamp('2024-11-01')
end_date = pd.Timestamp('2024-11-14')

# Filter the data based on the date range
df_filtered = df[(df[5] >= start_date) & (df[5] <= end_date)]

# Add a new 'date' column
df_filtered['date'] = df_filtered[5].dt.date

# Group by the 'date' column to count submissions
daily_submissions = df_filtered.groupby('date').size()

# Save the filtered data and daily submissions to CSV files
df_filtered.to_csv(filtered_file, index=False)
daily_submissions.to_csv(daily_submissions_file, header=['submissions'])

print(f"Filtered data saved to {filtered_file}")
print(f"Daily submissions saved to {daily_submissions_file}")
