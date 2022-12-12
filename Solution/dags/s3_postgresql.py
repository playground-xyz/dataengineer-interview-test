# Import required modules
import boto3
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook

# Define default arguments of DAG
default_args = {
    'owner' : 'issacekbote',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=2)
}

"""
Define postgres connection connection string
Prerequisites:
1. Create S3 bucket issplayground and upload files to the bucket
2. Create connection for AWS S3 and local postgresql in Airflow -> Admin -> Connections
"""
hook = PostgresHook(postgres_conn_id='localhost') # gets postgres credentials from airflow
conn = hook.get_conn()

cur = conn.cursor()

aws_connection = BaseHook.get_connection('playground_s3') # gets s3 credentials from airflow

AWS_ACCESS_KEY_ID = aws_connection.login # store S3 key
AWS_SECRET_ACCESS_KEY = aws_connection.password # store S3 access key

# Define AWS S3 connection string
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_connection.login,
    aws_secret_access_key=aws_connection.password
)


#parameters
# Define tables list. This is passed to the function data_load to iterate over tables for the data load
tables = [['region', 'region.tbl'], ['nation', 'nation.tbl'], ['part', 'part.tbl'], ['supplier', 'supplier.tbl'], ['partsupp', 'partsupp.tbl'],
           ['customer', 'customer.tbl'], ['orders', 'orders.tbl'], ['lineitem', 'lineitem.tbl']]
schema="playground" # define schema
bucket="issplayground" # define bucket

def data_load(cur, conn, tables, s3_client, schema, bucket):
    """
    The function takes cursor, postresql connection, tables list, s3 credentials, schema and bucket as parameters.
    The function iterates over the tables list, sets the table and bucket keys and then loads the into respecctive tables depending upon the condition.
    """

    for item in range(len(tables)):
        table = tables[item][0]  # set table name
        key = tables[item][1]   # set bucket key

        response = s3_client.get_object(Bucket=bucket, Key=key) # gets s3 connection response
 
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode") # gets s3 connection status
        # below code builds column names and insert query depending upon the table name
        if table == 'nation':
            col = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( n_nationkey, n_name, n_regionkey, n_comment ) \
                                VALUES (%s, %s, %s, %s) ON CONFLICT (n_nationkey) DO NOTHING; ")            

        elif table == 'region':
            col = ['r_regionkey', 'r_name', 'r_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( r_regionkey, r_name, r_comment ) \
                                VALUES (%s, %s, %s) ON CONFLICT (r_regionkey) DO NOTHING; ")

        elif table == 'part':
            col = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice',
                   'p_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice,\
                            p_comment ) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (p_partkey) DO NOTHING; ")  

        elif table == 'supplier':
            col = ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (s_suppkey) DO NOTHING; ")           
            
        elif table == 'partsupp':
            col = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) \
                                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (ps_partkey, ps_suppkey) DO NOTHING; ")

        elif table == 'customer':
            col = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                   'c_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, \
                                c_comment) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (c_custkey) DO NOTHING; ")

        elif table == 'orders':
            col = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                   'o_clerk',
                   'o_shippriority', 'o_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, \
                   o_clerk, o_shippriority, o_comment ) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (o_orderkey) DO NOTHING; ")

        else:
            col = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                   'l_discount',
                   'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                   'l_shipinstruct',
                   'l_shipmode', 'l_comment']
            insert_query = (f"INSERT INTO {schema}.{table}( l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, \
                   l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, \
                   l_shipmode, l_comment) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (l_orderkey, l_linenumber) DO NOTHING; ")

        # below code checks for the s3 connection status and reads data from the file
        if status == 200:
            print(f"Successful S3 get_object {key} response. Status - {status}")
            data = pd.read_csv(response.get("Body"), sep='|', names=col, header=None, index_col=False) # reads data into pandas dataframe
        else:
            print(f"Unsuccessful S3 get_object {key} response. Status - {status}")

        # below code prepares the data frame data to be loaded into postgres table
        table_df = data[col]
        #print(table_df)
        records = [tuple(x) for x in table_df.to_numpy()]
        #print(records)
        try:

            cur.executemany(insert_query, records)
            conn.commit()
            print(f"{table} data load completed")
        except Exception as error:

            print(f"Error while inserting data into {table} table", error)
            print("Exception TYPE:", type(error))
            print("\n")
            conn.rollback() 



def close_cursor (cur, conn):
    """
    The function takes cursor and connection as parameters.
    The function closes the connection to postgres database after the data load
    """    
    cur.close()
    conn.close()

#define DAG
"""
DAG is schedule to run every 15 minutes.
"""
dag = DAG('s3_postgresql',          
          description='Load and transform data ',
          start_date=datetime(2022, 12, 10, 8, 0, 0, 0),
          schedule_interval=timedelta(minutes=15),
          default_args=default_args,
        )

# create tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag) # dummpy task to begin the data load

# copy data from s3 to postgresql by calling function data_load
s3_to_postgres = PythonOperator (
    task_id='load_dw_data',    
    python_callable=data_load,
    op_kwargs={"cur": cur, "conn": conn, "tables": tables, "s3_client": s3_client, "schema":schema, "bucket":bucket},
    dag=dag,
)

# below task closes connection to postgres database
close_cursor= PythonOperator (
    task_id='close_cursor',
    dag=dag,
    python_callable=close_cursor,
    op_kwargs={"cur": cur, "conn": conn}
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag) # dummy task o end the data load

# task sequence
start_operator >> s3_to_postgres >> close_cursor >> end_operator

