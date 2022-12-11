# Import required modules
import psycopg2
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta



# Define default arguments of DAG
default_args = {
    'owner' : 'issacekbote',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=2)
}


"""
Define postgres connection connection string
Prerequisites:
1. Create connection for local postgresql in Airflow -> Admin -> Connections
"""
hook = PostgresHook(postgres_conn_id='localhost') # gets postgres credentials from airflow
conn = hook.get_conn() 

cur = conn.cursor()


def dw_customer(cur, conn, schema, target):
    """
    The function takes cursor, connection, schema and target as parameters to load data into data warehouse (dw).
    Target parameter is dw_customer table in dw where data is loaded for reporting and analysis.
    dw_customer table is truncated before the data is loaded.
    """
    dw_customer_truncate = f"TRUNCATE TABLE {schema}.{target};" # Truncate statement
    # Below is the insert statement to load data
    dw_customer_insert = f"INSERT INTO {schema}.{target}(c_custkey, c_nationkey, c_name, c_address, c_acctbal) \
                            SELECT c_custkey, c_nationkey, c_name, c_address, c_acctbal \
                            FROM {schema}.customer;"
    # Below code does error handling. If there is error in inserting, transaction is rolled back. If no error then transaction is commited
    try:
        cur.execute(dw_customer_truncate) # executes truncate query
        cur.execute(dw_customer_insert) # executes insert query
        conn.commit() # commits transaction
        print(f"{target} data load completed")
    # below code executes if there is error    
    except Exception as error:
        print(f"Error while inserting data into {target} table", error)
        print("Exception TYPE:", type(error))
        print("\n")
        conn.rollback() # rollbacks transaction

def dw_country(cur, conn, schema, target):
    """
    The function takes cursor, connection, schema and target as parameters to load data into data warehouse (dw).
    Target parameter is dw_country table in dw where data is loaded for reporting and analysis.
    dw_country table is truncated before the data is loaded.
    """    
    dw_country_truncate = f"TRUNCATE TABLE {schema}.{target};" # truncate statement
    # Below is the insert statement to load data
    dw_country_insert = f"INSERT INTO {schema}.{target} (n_nationkey, country, region) \
                            SELECT n_nationkey, n_name AS Country, r_name AS region FROM {schema}.nation \
                            INNER JOIN {schema}.region r ON r.r_regionkey = {schema}.nation.n_regionkey;"
    # Below code does error handling. If there is error in inserting, transaction is rolled back. If no error then transaction is commited
    try:
        cur.execute(dw_country_truncate) # executes truncate query
        cur.execute(dw_country_insert) # executes insert query
        conn.commit() # commits transaction
        print(f"{target} data load completed")
    # below code executes if there is error        
    except Exception as error:
        print(f"Error while inserting data into {target} table", error)
        print("Exception TYPE:", type(error))
        print("\n")
        conn.rollback() # rollbacks transation


def dw_lineitem(cur, conn, schema, target):
    """
    The function takes cursor, connection, schema and target as parameters to load data into data warehouse (dw).
    Target parameter is dw_lineitem table in dw where data is loaded for reporting and analysis.
    dw_lineitem table is appended with new data.
    """     
    # Below is the insert statement to load data
    dw_lineitem_insert = f"INSERT INTO {schema}.{target}(l_orderkey, c_custkey, n_nationkey, l_linenumber, l_quantity, o_totalprice, l_shipmode) \
                            SELECT l_orderkey, c_custkey, n_nationkey, l_linenumber, l_quantity, o_totalprice, l_shipmode FROM {schema}.lineitem \
                            INNER JOIN {schema}.orders o ON o.o_orderkey = {schema}.lineitem.l_orderkey \
                            INNER JOIN {schema}.customer c ON c.c_custkey = o.o_custkey \
                            INNER JOIN {schema}.nation n ON n.n_nationkey = c.c_nationkey \
                            ON CONFLICT (l_orderkey, c_custkey, n_nationkey, l_linenumber) DO NOTHING \
                            ;"
    # Below code does error handling. If there is error in inserting, transaction is rolled back. If no error then transaction is commited
    try:
        cur.execute(dw_lineitem_insert) # executes insert query
        conn.commit() # commits transaction
        print(f"{target} data load completed")
    except Exception as error:
        print(f"Error while inserting data into {target} table", error)
        print("Exception TYPE:", type(error))
        print("\n")
        conn.rollback() # rollbacks transaction

def dw_orders(cur, conn, schema, target):
    """
    The function takes cursor, connection, schema and target as parameters to load data into data warehouse (dw).
    Target parameter is dw_orders table in dw where data is loaded for reporting and analysis.
    dw_orders table is appended with new data.
    """    
    # Below is the insert statement to load data     
    dw_orders_insert = f"INSERT INTO {schema}.{target}(o_orderkey, o_custkey, o_totalprice, o_orderdate) \
                            SELECT o_orderkey, o_custkey, o_totalprice, o_orderdate FROM {schema}.orders \
                            ON CONFLICT (o_orderkey, o_custkey) DO NOTHING;"
    # Below code does error handling. If there is error in inserting, transaction is rolled back. If no error then transaction is commited
    try:
        #cur.execute(dw_orders_truncate)
        cur.execute(dw_orders_insert) # executes insert query
        conn.commit() # commits transaction
        print(f"{target} data load completed")
    except Exception as error:
        print(f"Error while inserting data into {target} table", error)
        print("Exception TYPE:", type(error))
        print("\n")
        conn.rollback() # rollback transaction


def data_load(cur, conn, tables):
    """
    The function takes cursor, connection and dictionary tables as parameters.
    The function iterates over tables dictionary and calls data load functions to load data based on the condition.
    """    
    for value in tables.values():
         target = value
         if target == 'dw_customer':
            dw_customer(cur, conn, schema='playground', target=target)

         elif target == 'dw_country':
             dw_country(cur, conn, schema='playground', target=target)

         elif target == 'dw_orders':
             dw_orders(cur, conn, schema='playground', target=target)

         elif target == 'dw_lineitem':
             dw_lineitem(cur, conn, schema='playground', target=target)



def close_cursor (cur, conn):
    """
    The function takes cursor and connection as parameters.
    The function closes the connection to postgres database after the data load
    """
    cur.close()
    conn.close()

# parameters
# below is the tables dictionary. This is passed to data_load function to iterate over tables for the data load
tables = {'customer': 'dw_customer', 'country': 'dw_country', 'lineitem': 'dw_lineitem', 'orders': 'dw_orders'}


#define DAG
"""
DAG is schedule to run daily @ 10 PM
"""
dag = DAG('dw_data_load',          
          description='Load and transform data ',
          start_date=datetime(2022, 12, 9, 22, 0, 0, 0),
          schedule_interval='@daily',
          default_args=default_args,
        )



# create tasks

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag) # dummpy task to begin the data load

# below task loads data into data warehouse by call python operator
dw_postgres = PythonOperator (
    task_id='load_dw_data',    
    python_callable=data_load,
    op_kwargs={"cur": cur, "conn": conn, "tables": tables},
    dag=dag,
)

# below task closes connection to postgres database
close_cursor= PythonOperator (
    task_id='close_cursor',
    dag=dag,
    python_callable=close_cursor,
    op_kwargs={"cur": cur, "conn": conn}
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag) # dummpy task to mark the end the data load

# task execution sequence
start_operator >> dw_postgres >> close_cursor >> end_operator