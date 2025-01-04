import pandas as pd
import re
import os
import psycopg2
from psycopg2 import sql

# Define a function to process a single markdown file
def process_markdown_file(file_path):
    # Read the content of the markdown file
    with open(file_path, 'r', encoding='utf-8') as file:
        markdown_content = file.read()

    # Define a regex pattern to capture section titles and their content
    pattern = r"### \*\*(?P<section_number>[\d\.\s\w]+)\*\*\s*(?P<section_content>(?:.|\n(?!### \*\*))+)"
    
    # Find all matches in the markdown content
    matches = re.finditer(pattern, markdown_content, re.MULTILINE)

    # Extract sections into a list of dictionaries
    sections = []
    for match in matches:
        section_number_and_title = match.group("section_number").strip()
        section_content = match.group("section_content").strip()
        sections.append({
            "Section Title": section_number_and_title,
            "Content": section_content,
            "File Name": os.path.basename(file_path)
        })

    return sections

# Define a function to process all markdown files in a directory
def process_markdown_files_in_directory(directory_path):
    all_sections = []
    
    # Log the files being processed
    print("Scanning directory for markdown files...")
    found_files = []

    # Iterate over all files in the directory
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.md'):
                file_path = os.path.join(root, file)
                found_files.append(file_path)
                sections = process_markdown_file(file_path)
                all_sections.extend(sections)

    # Log the found files
    print(f"Found {len(found_files)} markdown files:")
    for f in found_files:
        print(f" - {f}")

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(all_sections)

    return df

# Function to save the DataFrame to PostgreSQL using psycopg2
def save_to_postgresql(df, db_params, table_name):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS wizeobsidian.{table} (
            "Section Title" TEXT,
            "Content" TEXT,
            "File Name" TEXT
        )
    """).format(table=sql.Identifier(table_name))
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    for _, row in df.iterrows():
        insert_query = sql.SQL("""
            INSERT INTO wizeobsidian.{table} ("Section Title", "Content", "File Name")
            VALUES (%s, %s, %s)
        """).format(table=sql.Identifier(table_name))
        cursor.execute(insert_query, (row["Section Title"], row["Content"], row["File Name"]))
    
    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Data has been successfully saved to the table {table_name}.")

# Specify the directory containing the markdown files
directory_path = "C:/Users/larch/iCloudDrive/iCloud~md~obsidian/WizeCosm/00 - Univers/Les Races/"  # Replace with the path to your directory

# Process the markdown files and get the DataFrame
df = process_markdown_files_in_directory(directory_path)

# Print the DataFrame (for verification)
print(df)

# PostgreSQL connection parameters
db_params = {
    "dbname": "wizecosm_NAS",      # Replace with your database name
    "user": "dr0ant",          # Replace with your username
    "password": "°889",      # Replace with your password
    "host": "100.72.70.102",         # Replace with your host (e.g., "localhost" or IP address)
    "port": "5433"               # Replace with your port, default is 5432
}

# Specify the table name in PostgreSQL
table_name = "races"  # Replace with your desired table name

# Save the DataFrame to PostgreSQL
save_to_postgresql(df, db_params, table_name)
