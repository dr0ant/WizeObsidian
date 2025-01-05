import pandas as pd
import re
import os
import psycopg2
from psycopg2 import sql

# Define a function to process a single markdown file
def process_markdown_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        markdown_content = file.read()

    pattern = r"### \*\*(?P<section_number>[\d\.\s\w]+)\*\*\s*(?P<section_content>(?:.|\n(?!### \*\*))+)"

    matches = re.finditer(pattern, markdown_content, re.MULTILINE)

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
    print("Scanning directory for markdown files...")
    found_files = []

    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.md'):
                file_path = os.path.join(root, file)
                found_files.append(file_path)
                sections = process_markdown_file(file_path)
                all_sections.extend(sections)

    print(f"Found {len(found_files)} markdown files:")
    for f in found_files:
        print(f" - {f}")

    df = pd.DataFrame(all_sections)
    return df

# Function to save the DataFrame to PostgreSQL using psycopg2
def save_to_postgresql(df, db_params, table_name):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS wizeobsidian.{table} (
            "Section Title" TEXT NOT NULL,
            "Content" TEXT,
            "File Name" TEXT NOT NULL,
            UNIQUE ("Section Title", "File Name")
        )
    """).format(table=sql.Identifier(table_name))
    cursor.execute(create_table_query)
    conn.commit()

    for _, row in df.iterrows():
        merge_query = sql.SQL("""
            INSERT INTO wizeobsidian.{table} ("Section Title", "Content", "File Name")
            VALUES (%s, %s, %s)
            ON CONFLICT ("Section Title", "File Name")
            DO UPDATE SET "Content" = EXCLUDED."Content"
        """).format(table=sql.Identifier(table_name))
        cursor.execute(merge_query, (row["Section Title"], row["Content"], row["File Name"]))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Data has been successfully merged into the table {table_name}.")

# Specify the directory containing the markdown files
directory_path = "C:/Users/larch/iCloudDrive/iCloud~md~obsidian/WizeCosm/00 - Univers/Les Races/" 

df = process_markdown_files_in_directory(directory_path)

print(df)

# PostgreSQL connection parameters
db_params = {
    "dbname": "wizecosm_NAS",
    "user": "dr0ant",
    "password": "°889",
    "host": "100.72.70.102",
    "port": "5433"
}

table_name = "races"

save_to_postgresql(df, db_params, table_name)
