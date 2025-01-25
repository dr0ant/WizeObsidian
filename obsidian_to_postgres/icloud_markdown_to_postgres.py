from pyicloud import PyiCloudService
import pandas as pd
import re
import psycopg2
from psycopg2 import sql
import os
import json
import getpass
from shutil import copyfileobj

class MarkdownToPostgres:
    def __init__(self, db_config_path, icloud_username, icloud_password):
        """
        Initialize the class with database connection parameters and authenticate to iCloud.

        :param db_config_path: Path to the JSON file containing database parameters.
        :param icloud_username: iCloud username (email).
        :param icloud_password: iCloud password.
        """
        self.db_params = self._load_db_params(db_config_path)
        self.icloud = self._authenticate_icloud(icloud_username, icloud_password)

    def _load_db_params(self, db_config_path):
        with open(db_config_path, 'r', encoding='utf-8') as json_file:
            db_params = json.load(json_file)
        return db_params

    def _authenticate_icloud(self, username, password):
        icloud = PyiCloudService(username, password)
        if icloud.requires_2fa:
            print("Two-factor authentication required. Please enter the code:")
            code = input("Enter the 2FA code: ")
            if not icloud.validate_2fa_code(code):
                raise Exception("Invalid 2FA code.")
        return icloud

    def _get_icloud_files_and_folders(self, folder_path):
        """
        Recursively fetch all files and folders from iCloud, similar to the Ubiquity API style.
        
        :param folder_path: Path to the folder in iCloud Drive.
        :return: Dictionary with file names as keys and file content as values.
        """
        paths = {}
        folder = self.icloud.drive[folder_path]
        
        # Recursively check all subfolders and files
        def scan_folder(current_folder):
            print(f"Scanning folder: {current_folder}")  # Debugging line
            for item in current_folder:
                try:
                    # Normalize the path to use forward slashes
                    item_path = f"{folder_path}/{item.name}".replace("\\", "/")
                    print(f"Found item: {item_path}")  # Debugging line
                    
                    if item.is_dir:
                        scan_folder(item)  # Recurse if it's a directory
                    else:
                        paths[item_path] = item  # It's a file
                except Exception as e:
                    print(f"Error processing item {item}: {e}")

        scan_folder(folder)  # Start scanning from the root folder
        return paths

    def _get_markdown_files_from_icloud(self, folder_path):
        """
        Fetch markdown files from iCloud recursively.

        :param folder_path: Path to the folder in iCloud Drive.
        :return: Dictionary with file names as keys and file content as values.
        """
        markdown_files = {}
        paths = self._get_icloud_files_and_folders(folder_path)
        
        # Now retrieve the content of markdown files
        for file_path, file in paths.items():
            if file_path.endswith('.md'):  # Check if it is a markdown file
                try:
                    file_content = file.download().decode('utf-8')  # Download and decode file content
                    markdown_files[file_path] = file_content
                except Exception as e:
                    print(f"Error downloading file {file_path}: {e}")
        print(f"Loaded {len(markdown_files)} markdown files.")
        return markdown_files

    def process_markdown_content(self, file_name, file_content):
        """
        Process markdown content to extract sections and content.

        :param file_name: Name of the markdown file.
        :param file_content: Content of the markdown file.
        :return: List of dictionaries with section details.
        """
        pattern = r"### \*\*(?P<section_number>[\d\.\s\w]+)\*\*\s*(?P<section_content>(?:.|\n(?!### \*\*))+)"
        
        matches = re.finditer(pattern, file_content, re.MULTILINE)
        sections = []
        for match in matches:
            sections.append({
                "Section Title": match.group("section_number").strip(),
                "Content": match.group("section_content").strip(),
                "File Name": file_name
            })
        return sections

    def process_markdown_files_from_icloud(self, folder_path):
        """
        Process all markdown files from a folder in iCloud Drive.

        :param folder_path: Path to the folder in iCloud Drive.
        :return: DataFrame with all extracted sections.
        """
        markdown_files = self._get_markdown_files_from_icloud(folder_path)
        all_sections = []
        for file_name, file_content in markdown_files.items():
            sections = self.process_markdown_content(file_name, file_content)
            all_sections.extend(sections)
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

if __name__ == "__main__":
    # Prompt for database configuration file
    db_config_path = input("Enter the path to the database configuration JSON file (e.g., db_config.json): ").strip()
    if not os.path.exists(db_config_path):
        print(f"Error: The file '{db_config_path}' does not exist.")
        exit(1)

    # Prompt for iCloud credentials
    icloud_username = input("Enter your iCloud email: ").strip()
    icloud_password = getpass.getpass("Enter your iCloud password: ").strip()

    try:
        # Initialize the class
        print("\nInitializing MarkdownToPostgres...")
        markdown_to_pg = MarkdownToPostgres(db_config_path, icloud_username, icloud_password)

        # List available paths in iCloud Drive
        print("\nListing all files and folders inside the 'Obsidian' folder:")
        paths = markdown_to_pg._get_icloud_files_and_folders("Obsidian")
        for idx, path in enumerate(paths, 1):
            print(f"{idx}. {path}")

        # Select a folder by number
        folder_choice = int(input("Select a folder by number: ").strip())
        selected_folder = list(paths.keys())[folder_choice - 1]
        print(f"\nSelected folder: {selected_folder}")

        # Debugging: Check the path before accessing the folder
        print(f"Accessing iCloud folder: {selected_folder}")
        
        # Get markdown files from the selected folder
        sections_df = markdown_to_pg.process_markdown_files_from_icloud(selected_folder)

        # Display the extracted sections
        print("\nExtracted Sections:")
        print(sections_df)

        # Ask for PostgreSQL table name
        table_name = input("Enter the PostgreSQL table name to store the data (e.g., markdown_sections): ").strip()

        # Save to PostgreSQL
        print(f"\nSaving data to the PostgreSQL table '{table_name}'...")
        markdown_to_pg.save_to_postgresql(sections_df, table_name)

        print("\nExecution completed successfully!")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
