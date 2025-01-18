import os
import json
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd
from dagster import job, op, Out, Output, In, Nothing
import psycopg2
from psycopg2.extras import execute_values
import subprocess

# Define ops
@op(out=Out(pd.DataFrame))
def load_md_files() -> pd.DataFrame:
    base_path = Path("C:/Users/larch/iCloudDrive/iCloud~md~obsidian/WizeCosm/") 
    data = []

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith(".md"):
                file_path = Path(root) / file
                rel_path = file_path.relative_to(base_path)
                parts = list(rel_path.parts)
                content = file_path.read_text(encoding="utf-8")
                is_more_than_20_char = len(content.strip()) > 20

                # Add up to 5 folder levels plus file name and boolean
                row = parts[:5] + ["" for _ in range(5 - len(parts))] + [file, is_more_than_20_char]
                data.append(row)

    # Create DataFrame
    columns = [f"folder{i+1}" for i in range(5)] + ["file_name", "is_more_than_20_char"]
    print(pd.DataFrame(data, columns=columns))
    return pd.DataFrame(data, columns=columns)

@op(ins={"df": In(pd.DataFrame)})
def merge_to_postgres(df: pd.DataFrame):
    # Load PostgreSQL connection parameters
    config_path = Path("postgres_params.json")
    with open(config_path, "r" , encoding='utf-8') as f:
        params = json.load(f)
       
    # Add an update_date column
    df["update_date"] = datetime.utcnow()

    # Create or merge table in PostgreSQL
    create_table_query = """
    CREATE TABLE IF NOT EXISTS wizeobsidian.md_file_analysis (
        folder1 TEXT,
        folder2 TEXT,
        folder3 TEXT,
        folder4 TEXT,
        folder5 TEXT,
        file_name TEXT,
        is_more_than_20_char BOOLEAN,
        update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (folder1, folder2, folder3, folder4, folder5, file_name)
    )"""

    # PostgreSQL connection
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cur:
            # Create table if not exists
            cur.execute(create_table_query)

            # Merge data into the table
            insert_query = """
            INSERT INTO wizeobsidian.md_file_analysis (folder1, folder2, folder3, folder4, folder5, file_name, is_more_than_20_char, update_date)
            VALUES %s
            ON CONFLICT (folder1, folder2, folder3, folder4, folder5, file_name)
            DO UPDATE SET
                is_more_than_20_char = EXCLUDED.is_more_than_20_char,
                update_date = EXCLUDED.update_date
            """
            execute_values(
                cur,
                insert_query,
                df.values.tolist()
            )
        conn.commit()

@op
def trigger_dbt_flow():
    # Define the path to your dbt project and the dbt command
    dbt_project_path = Path("C:/Users/larch/SynologyDrive/WizeObsidian/dbt/wizeobsidian_project")
    
    # Run dbt run command
    dbt_run_command = ["dbt", "run", "--models", "reporting_analysis_obsidian_WizeCosm"]
    result = subprocess.run(dbt_run_command, cwd=dbt_project_path, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"DBT run failed: {result.stderr}")
    else:
        print(f"DBT run succeeded: {result.stdout}")

    # Run dbt docs generate command
    dbt_docs_command = ["dbt", "docs", "generate"]
    result_docs = subprocess.run(dbt_docs_command, cwd=dbt_project_path, capture_output=True, text=True)

    if result_docs.returncode != 0:
        raise Exception(f"DBT docs generate failed: {result_docs.stderr}")
    else:
        print(f"DBT docs generated successfully: {result_docs.stdout}")

@op
def copy_docs_to_target():
    # Define the source and destination paths
    source_dir = Path("C:/Users/larch/SynologyDrive/WizeObsidian/dbt/wizeobsidian_project/target")
    destination_dir = Path("C:/Users/larch/SynologyDrive/WizeObsidian/docs")

    # Recursively copy all files from source to destination
    if source_dir.exists() and source_dir.is_dir():
        for item in source_dir.rglob('*'):
            if item.is_file():
                relative_path = item.relative_to(source_dir)
                target_path = destination_dir / relative_path

                # Ensure destination directory exists
                target_path.parent.mkdir(parents=True, exist_ok=True)

                # Copy the file, replace backslashes with forward slashes
                shutil.copy(item, target_path)

        print(f"Copied contents from {source_dir} to {destination_dir}")
    else:
        print(f"Source directory {source_dir} does not exist.")

@job
def md_file_analysis_job():
    # Load markdown files, then merge to Postgres, trigger dbt flow, and finally copy docs
    df = load_md_files()
    merge_to_postgres(df)
    trigger_dbt_flow()
    copy_docs_to_target()
