#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import os

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--file-path', required=True, help='Path to the local parquet file')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Number of rows per insert')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, file_path, target_table, chunksize):
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f"Reading {file_path}...")
    # Load the entire parquet file (Parquet is very memory efficient)
    df = pd.read_parquet(file_path)

    # Create the table schema (replaces existing table)
    print(f"Initializing table {target_table}...")
    df.head(0).to_sql(name=target_table, con=engine, if_exists='replace')

    # Insert data in chunks
    print(f"Inserting data in chunks of {chunksize}...")
    for i in tqdm(range(0, len(df), chunksize)):
        chunk = df.iloc[i:i+chunksize]
        chunk.to_sql(name=target_table, con=engine, if_exists='append')

    print("Successfully ingested data into PostgreSQL.")

if __name__ == '__main__':
    run()