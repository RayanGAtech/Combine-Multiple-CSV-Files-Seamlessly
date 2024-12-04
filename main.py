import os
import pandas as pd

# Specify the directory containing the CSV files
csv_directory = 'data/'  # Replace with the actual path to your directory

# Specify the output path for the combined CSV
output_csv = 'output/combined.csv'  # Replace with the desired output path

# List all files in the directory
all_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

# Ensure the output file doesn't exist to avoid appending to an old file
if os.path.exists(output_csv):
    os.remove(output_csv)

# Define the correct header for the output CSV
correct_header = ['p1', 'p2', 'p3', 'p4']

# Variable to track if the header has been written
header_written = False

# Iterate over all CSV files and append each to the output file
for idx, file in enumerate(all_files):
    file_path = os.path.join(csv_directory, file)
    
    try:
        # Read the CSV in chunks (for large files), assuming no header in the CSV
        for chunk in pd.read_csv(file_path, chunksize=10000, header=None):  # Using header=None to avoid wrong headers
            # Add the correct header to the chunk
            chunk.columns = correct_header  # Explicitly set the correct header
            chunk['file'] = file  # Add the 'file' column to track source
            
            # Write the first chunk with the header
            if not header_written:
                chunk.to_csv(output_csv, index=False, mode='w', header=True)
                header_written = True
            else:
                # Write subsequent chunks without the header
                chunk.to_csv(output_csv, index=False, mode='a', header=False)
    except pd.errors.EmptyDataError:
        print(f"Skipping empty file: {file_path}")
        continue
    
    print(f"Processed {file}")

print(f"All CSV files have been combined into {output_csv}")
