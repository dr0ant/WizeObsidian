import pandas as pd
import re
import os
import psycopg2
from psycopg2 import sql
import json




class Markdown_to_Postgres:
    def __init__(self, db_config_path):
        """
        Initialize the class with database connection parameters.

        :param db_config_path: Path to the JSON file containing database parameters.
        """
        self.db_params = self._load_db_params(db_config_path)

    def _load_db_params(self, db_config_path):
        """
        Load PostgreSQL connection parameters from a JSON file.

        :param db_config_path: Path to the JSON file containing database parameters.
        :return: Dictionary with database parameters.
        """
        with open(db_config_path, 'r', encoding='utf-8') as json_file:
            db_params = json.load(json_file)
            print("Connecting to PostgreSQL with the following parameters:")
            print({key: db_params[key] for key in db_params})
        return db_params

    def process_markdown_file(self, file_path):
        """
        Process a single markdown file to extract sections and content.

        :param file_path: Path to the markdown file.
        :return: List of dictionaries with section details.
        """
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

    def process_markdown_files_in_directory(self, directory_path):
        """
        Process all markdown files in a directory to extract sections and content.

        :param directory_path: Path to the directory containing markdown files.
        :return: DataFrame with all extracted sections.
        """
        all_sections = []
        print("Scanning directory for markdown files...")
        found_files = []

        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.md'):
                    file_path = os.path.join(root, file)
                    found_files.append(file_path)
                    sections = self.process_markdown_file(file_path)
                    all_sections.extend(sections)

        print(f"Found {len(found_files)} markdown files:")
        for f in found_files:
            print(f" - {f}")

        return pd.DataFrame(all_sections)

    def save_to_postgresql(self, df, table_name):
        """
        Save a DataFrame to PostgreSQL, creating or merging records as needed.

        :param df: DataFrame containing the data to save.
        :param table_name: Name of the table in PostgreSQL.
        """
        conn = psycopg2.connect(**self.db_params)
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
