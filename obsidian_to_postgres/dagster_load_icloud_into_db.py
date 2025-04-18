from dagster import job, op, In, Out, Field, Nothing, get_dagster_logger
import pandas as pd
import sys
import os
import json
sys.path.append(os.path.abspath("obsidian_to_postgres"))  # Adjust the path as needed
from Markdown_to_postgres import Markdown_to_Postgres
from icloud_md_files_getter import ICloudFileGetter

@op(
    config_schema={
        "icloud_folders_to_scan": Field(list, description="List of iCloud folder paths to scan"),
        "icloud_params_path": Field(str, description="Path to the JSON file containing iCloud credentials"),
        "db_config_path": Field(str, description="Path to the JSON file containing database parameters"),
    },
    out=Out(pd.DataFrame, description="Extracted markdown data as a DataFrame")
)
def process_markdown_files_from_icloud(context):
    """
    Fetch markdown files from iCloud, process them, and return the resulting DataFrame.
    """
    icloud_folders_to_scan = context.op_config["icloud_folders_to_scan"]
    icloud_params_path = context.op_config["icloud_params_path"]
    db_config_path = context.op_config["db_config_path"]

    logger = get_dagster_logger()
    logger.info(f"Step: Starting to fetch markdown files from iCloud folders: {icloud_folders_to_scan}")

    # Load iCloud credentials
    with open(icloud_params_path, "r") as params_file:
        icloud_params = json.load(params_file)
        icloud_username = icloud_params["email"]
        icloud_password = icloud_params["password"]

    # Initialize the iCloud file getter
    file_getter = ICloudFileGetter(icloud_username, icloud_password, icloud_folders_to_scan)

    # Fetch file content
    files_json = file_getter.list_root_folders()

    # Ensure files_json is a dictionary
    if isinstance(files_json, str):
        try:
            files_json = json.loads(files_json)  # Convert JSON string to dictionary
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            raise ValueError("The response from list_root_folders() is not valid JSON.")

    # Process the markdown files using process_markdown_from_json
    markdown_processor = Markdown_to_Postgres(db_config_path)
    all_sections = []
    for file_name, content in files_json.items():
        sections = markdown_processor.process_markdown_from_json(file_name, content)
        all_sections.extend(sections)

    df = pd.DataFrame(all_sections)
    logger.info(f"Step: Extracted {len(df)} records from iCloud folders")
    logger.info(f"Step: Finished processing markdown files from iCloud")
    return df


@op(
    ins={"df": In(pd.DataFrame, description="DataFrame to save")},
    config_schema={
        "table_name": Field(str, description="Name of the PostgreSQL table"),
        "db_config_path": Field(str, description="Path to the JSON file containing database parameters"),
    },
    out=Out(Nothing, description="Indicates completion of saving data")
)
def save_to_postgresql(context, df):
    """
    Save the DataFrame to a PostgreSQL table.
    """
    table_name = context.op_config["table_name"]
    db_config_path = context.op_config["db_config_path"]

    logger = get_dagster_logger()
    logger.info(f"Step: Starting to save data to table: {table_name}")

    markdown_processor = Markdown_to_Postgres(db_config_path)
    markdown_processor.save_to_postgresql(df, table_name)

    logger.info(f"Step: Data successfully saved to table: {table_name}")
    logger.info(f"Step: Finished saving data to table: {table_name}")


@job
def load_markdowns_into_db():
    """
    Main Dagster job to process markdown files from iCloud and save them into PostgreSQL tables.
    """
    flows = [
        {
            "table_name": "races",
            "icloud_folders_to_scan": [['Obsidian', 'WizeCosm', '00 - Univers', 'Les Races']],
        },
        {
            "table_name": "politics",
            "icloud_folders_to_scan": [['Obsidian', 'WizeCosm', '00 - Univers', 'Politique']],
        },
        {
            "table_name": "continents",
            "icloud_folders_to_scan": [['Obsidian', 'WizeCosm', '01 - Géographie', 'Continents']],
        },
        # Add more flows here...
    ]

    icloud_params_path = "icloud_params.json"
    db_config_path = "postgres_params.json"

    logger = get_dagster_logger()
    logger.info("Step: Starting the load_markdowns_into_db job")

    for flow in flows:
        logger.info(f"Step: Starting processing for flow: table_name={flow['table_name']}")

        process_op = process_markdown_files_from_icloud.configured(
            {
                "icloud_folders_to_scan": flow["icloud_folders_to_scan"],
                "icloud_params_path": icloud_params_path,
                "db_config_path": db_config_path,
            },
            name=f"process_{flow['table_name']}",
        )
        save_op = save_to_postgresql.configured(
            {
                "table_name": flow["table_name"],
                "db_config_path": db_config_path,
            },
            name=f"save_{flow['table_name']}",
        )

        logger.info(f"Step: Executing process operation for table: {flow['table_name']}")
        save_op(process_op())
        logger.info(f"Step: Completed processing for flow: table_name={flow['table_name']}")

    logger.info("Step: Finished the load_markdowns_into_db job")