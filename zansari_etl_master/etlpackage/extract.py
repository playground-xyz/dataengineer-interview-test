# Abdullah Zaid 2022/11/30
# UpZip data.zip file
# Create Raw DB
# Load tbl files to Raw DB

import zipfile
import pandas as pd
from sqlalchemy import text, inspect

def unzip_data_file(zipfile_source_location,unzip_target_location):
    try:
        with zipfile.ZipFile(zipfile_source_location, 'r') as zip_ref:
            zip_ref.extractall(unzip_target_location)
    except:
        raise ValueError("Invalid source or target location")
    return f"zip file  extracted from {zipfile_source_location} to {unzip_target_location}"

def run_create_table_script(engine, query_file_location):
    try:
        with engine.connect() as conn:
            with open(query_file_location) as file:
                queries = ''.join(file.readlines()).split(';')
                for query in queries:
                    exec_query = text(query)
                    conn.execute(exec_query)
    except:
        raise Exception(f"Cannot connect to {engine} or no file found at {query_file_location}")
    return f"Tables created from {query_file_location}"


def add_data_to_tables(table, engine):
    try:
        table_name = table.split('.')[0]
        insp = inspect(engine)
        columns = [row['name'] for row in insp.get_columns(table_name)]
        print(f"tryng to read {table} with {columns} in {engine}")
        df = pd.read_table(f'tables/{table}',index_col = False, sep = '|' ,names = columns)
        df = df.iloc[:, :len(columns)]
        df.to_sql(table_name, con = engine, index = False, index_label = "id", if_exists = 'append')
    except:
        raise Exception(f"Cannot connect to {engine}")
    return f"{table} created at {engine}"
