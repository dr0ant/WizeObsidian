from dagster import job, op, In, Out, Field, Nothing, get_dagster_logger
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath("obsidian_to_postgres"))  # Adjust the path as needed
from Markdown_to_postgres import Markdown_to_Postgres


@op(
    config_schema={
        "directory_path": Field(str, description="Path to the directory containing markdown files"),
        "db_config_path": Field(str, description="Path to the JSON file containing database parameters"),
    },
    out=Out(pd.DataFrame, description="Extracted markdown data as a DataFrame")
)
def process_markdown_files(context):
    """
    Process markdown files in a given directory and return the resulting DataFrame.
    """
    directory_path = context.op_config["directory_path"]
    db_config_path = context.op_config["db_config_path"]

    logger = get_dagster_logger()
    logger.info(f"Step: Starting to process markdown files in directory: {directory_path}")

    markdown_processor = Markdown_to_Postgres(db_config_path)
    df = markdown_processor.process_markdown_files_in_directory(directory_path)

    logger.info(f"Step: Extracted {len(df)} records from directory: {directory_path}")
    logger.info(f"Step: Finished processing markdown files in directory: {directory_path}")
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
    Main Dagster job to process markdown files and save them into PostgreSQL tables.
    """
    flows = [
       {"table_name": "races", "directory_path": "/Users/antoinelarcher/Library/Mobile Documents/iCloud~md~obsidian/Documents/WizeCosm/00 - Univers/Les Races/"},
       {"table_name": "politics", "directory_path": "/Users/antoinelarcher/Library/Mobile Documents/iCloud~md~obsidian/Documents/WizeCosm/00 - Univers/Politique/"},
       {"table_name": "continents", "directory_path": "/Users/antoinelarcher/Library/Mobile Documents/iCloud~md~obsidian/Documents/WizeCosm/01 - Géographie/Continents/"},
        # Add more flows here...
    ]

    db_config_path = "postgres_params.json"

    logger = get_dagster_logger()
    logger.info("Step: Starting the load_markdowns_into_db job")

    for flow in flows:
        logger.info(f"Step: Starting processing for flow: table_name={flow['table_name']}, directory_path={flow['directory_path']}")

        process_op = process_markdown_files.configured(
            {
                "directory_path": flow["directory_path"],
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
