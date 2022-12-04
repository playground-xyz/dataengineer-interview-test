# Abdullah Zaid 2022/11/30
# Create conformed and semantic dbs
# Load tranformed tables to conformed and start schema tables to semantic
import pandas as pd

def transform_and_load_table(source_engine, target_engine, table_name, query):
    try:
        source_conn = source_engine.connect()
        df = pd.read_sql(query,source_conn)
        target_conn = target_engine.connect()
        df.to_sql(table_name, target_conn, index = False, index_label= 'id', if_exists ='replace')
    except:
        raise ValueError("Unable to connect to these engines")
    return f"Created {table_name} in {target_engine}."