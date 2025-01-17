from Markdown_to_postgres import Markdown_to_Postgres

def main():
    db_config_path = "postgres_params.json"

    # Define the flows: table_name and corresponding directory path
    flows = [
        {"table_name": "races", "directory_path": "C:/Users/larch/iCloudDrive/iCloud~md~obsidian/WizeCosm/00 - Univers/Les Races/"},
        {"table_name": "politics", "directory_path": "C:/Users/larch/iCloudDrive/iCloud~md~obsidian/WizeCosm/00 - Univers/Politique/"},
        
        # Add more flows here...
    ]

    # Initialize the Markdown_to_Postgres class
    markdown_processor = Markdown_to_Postgres(db_config_path)

    for flow in flows:
        print(f"Processing flow for table '{flow['table_name']}' and directory '{flow['directory_path']}'...")
        df = markdown_processor.process_markdown_files_in_directory(flow["directory_path"])
        markdown_processor.save_to_postgresql(df, flow["table_name"])

if __name__ == "__main__":
    main()
