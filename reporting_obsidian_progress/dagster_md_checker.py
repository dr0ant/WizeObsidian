import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from dagster import job, op, Out, Output, In, Nothing
import psycopg2
from psycopg2.extras import execute_values

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

@job
def md_file_analysis_job():
    merge_to_postgres(load_md_files())
