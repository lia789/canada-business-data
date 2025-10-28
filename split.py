import csv
import os

INPUT_FILE = "google_map_queries_new_categories.csv"
PARTS = 5


def split_csv(input_file, num_parts):
    
    # Read all rows from the CSV file
    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        rows = list(reader)

    
    # Get header row (if exists)
    header = rows[0] if rows else []
    data_rows = rows[1:] if len(rows) > 1 else []
    total_rows = len(data_rows)

    

    
    # Calculate rows per part
    rows_per_part = total_rows // num_parts
    remainder = total_rows % num_parts

    if remainder > 0:
        print(f"First {remainder} parts will have 1 extra row")
    
    # Split and write the parts
    start_idx = 0
    
    for part_num in range(num_parts):
        current_part_size = rows_per_part + (1 if part_num < remainder else 0)
        end_idx = start_idx + current_part_size
        
        # Create output filename
        base_name = os.path.splitext(input_file)[0]
        extension = os.path.splitext(input_file)[1]
        output_file = f"{base_name}_part_{part_num + 1}{extension}"
        
        # Write this part to file
        with open(output_file, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            
            # Write header if it exists
            if header:
                writer.writerow(header)
            
            # Write data rows for this part
            for row_idx in range(start_idx, end_idx):
                if row_idx < len(data_rows):
                    writer.writerow(data_rows[row_idx])
        start_idx = end_idx

if __name__ == "__main__":
    split_csv(INPUT_FILE, PARTS)

