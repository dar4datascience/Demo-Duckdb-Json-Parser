import duckdb
import os
from pathlib import Path
from typing import List
import argparse

def parse_welocalize_json(file_paths: List[str], output_dir: str = "./output"):
    """
    Parses a list of JSON files (expected in xAPI statement format),
    flattens key fields into a tabular format using DuckDB, and exports them to CSV.

    Parameters:
    - file_paths: List of strings representing paths to JSON files
    - output_dir: Directory where the output CSVs will be saved (default: "./output")
    """
    
    os.makedirs(output_dir, exist_ok=True)

    for file_path in file_paths:
        base_name = Path(file_path).stem
        output_file = os.path.join(output_dir, f"{base_name}.csv")

        con = duckdb.connect(database=':memory:')
        con.execute(f"""
            CREATE OR REPLACE TABLE flat_statements AS
            SELECT
                s.id AS statement_id,
                s.timestamp,
                s.stored,
                s.actor.name AS actor_name,
                s.actor.objectType AS actor_object_type,
                s.actor.account.name AS actor_email,
                s.actor.account.homepage AS actor_home_page,
                s.verb.id AS verb_id,
                s.verb.display['en-US'] AS verb_display,
                s.object.id AS object_id,
                s.object.objectType AS object_type,
                COALESCE(s.object.definition.name['und'], s.object.definition.name['en-US'], '') AS object_name,
                s.object.definition.description['und'] AS object_description,
                s.context.registration AS registration,
                s.context.contextActivities.parent[1].id AS parent_id,
                s.context.contextActivities.parent[1].objectType AS parent_object_type,
                s.context.contextActivities.grouping[1].id AS grouping_id,
                s.context.contextActivities.grouping[1].objectType AS grouping_object_type,
                s.authority.name AS authority_name,
                s.authority.objecttype AS authority_object_type,
                s.authority.account.name AS authority_email,
                s.authority.account.homepage AS authority_home_page
            FROM (
                SELECT 
                    UNNEST(statements) AS s,
                    more --- ASSUMING is next page, so its only relevant for getting the full data before parsing a json
                FROM read_json_auto('{file_path}')
            )
        """)

        con.execute(f"COPY flat_statements TO '{output_file}' (HEADER, DELIMITER ',')")
        print(f"✅ Exported: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flatten xAPI JSON files and export them to CSV using DuckDB.")
    parser.add_argument("file_paths", nargs="+", help="Path(s) to the input JSON file(s).")
    parser.add_argument("--output_dir", default="./output", help="Directory where output CSVs will be saved.")
    args = parser.parse_args()

    parse_welocalize_json(args.file_paths, args.output_dir)

# Example terminal usage:
# python demo_duckdb.py data1.json --output_dir=flattened_csvs
