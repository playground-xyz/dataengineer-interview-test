import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from etlpackage import extract, transform

zipfile_source_location = '../data.zip'
query_file_location = "../ddl.sql"
unzip_target_location = 'tables'

conformed_engine = create_engine('sqlite:///conformed.db', echo = False)
semantic_engine = create_engine('sqlite:///semantic.db', echo = False)

if sys.argv[1] == "data_gen":
    raw_engine = create_engine('sqlite:///TPCH-sqlite/TPC-H.db', echo = False)
    
else:
    raw_engine = create_engine('sqlite:///raw.db', echo = False)
    # unzip data.ztip file and extract all tables data
    res = extract.unzip_data_file(zipfile_source_location,unzip_target_location )
    print(res)

    # Initiate all table using the ddl.sql file
    res = extract.run_create_table_script(raw_engine, query_file_location)
    print(res)
    
    # get list of all tables extracted and add data from them into created tables
    tables = os.listdir(unzip_target_location)
    print(tables)
    for table in tables:
        if table.split('.')[1] != 'tbl':
            continue
        res = extract.add_data_to_tables(table, raw_engine)
        print(res)




####################################################################################################################################################################################################################################

## Q1. Add a Column to Group Customers into 3 Groups
table_name = "customer"
query = (f"""SELECT *, CASE WHEN c_acctbal < 0 THEN "HIGH RISK" WHEN c_acctbal <1000 THEN "MEDIUM RISK" ELSE "LOW RISK" END AS C_ACCTBAL_GROUP  FROM customer""")
res = transform.transform_and_load_table(raw_engine, conformed_engine, table_name, query)
print(res)

# Q2. Add Revenue per line Item AND Q3. Distribute Dates over Last 2 years
table_name = "lineitem"
query = (f"""
    SELECT 
    L_ORDERKEY
    , L_PARTKEY
    , L_SUPPKEY
    , L_LINENUMBER
    , L_QUANTITY
    , L_EXTENDEDPRICE
    , L_DISCOUNT
    , L_TAX
    , L_RETURNFLAG
    , L_LINESTATUS
    , STRFTIME('%Y-%m-%d',STRFTIME('%s','now') -  ((STRFTIME('%s','now') - STRFTIME('%s',L_SHIPDATE))%63115200), 'unixepoch') AS L_SHIPDATE
    , STRFTIME('%Y-%m-%d',STRFTIME('%s','now') -  ((STRFTIME('%s','now') - STRFTIME('%s',L_COMMITDATE))%63115200), 'unixepoch') AS L_COMMITDATE
    , STRFTIME('%Y-%m-%d',STRFTIME('%s','now') -  ((STRFTIME('%s','now') - STRFTIME('%s',L_RECEIPTDATE))%63115200), 'unixepoch') AS L_RECEIPTDATE
    , L_SHIPINSTRUCT
    , L_SHIPMODE
    , L_COMMENT 
    , L_QUANTITY * L_EXTENDEDPRICE AS L_GROSS_REVENUE
    , L_QUANTITY * L_EXTENDEDPRICE * (1-L_DISCOUNT-L_TAX) AS L_NET_REVENUE 
    FROM LINEITEM""")
res = transform.transform_and_load_table(raw_engine, conformed_engine, table_name, query)
print(res)

# Q3.Convert Dates to be distributed over last two years for orders
table_name = "orders"
query = f"""
        SELECT 
        O_ORDERKEY
        , O_CUSTKEY
        , O_ORDERSTATUS
        , O_TOTALPRICE
        , STRFTIME('%Y-%m-%d',STRFTIME('%s','now') -  ((STRFTIME('%s','now') - STRFTIME('%s',O_ORDERDATE))%63115200), 'unixepoch') AS O_ORDERDATE
        , O_ORDERPRIORITY
        , O_CLERK
        , O_SHIPPRIORITY
        , O_COMMENT
        FROM ORDERS
    """
res = transform.transform_and_load_table(raw_engine, conformed_engine, table_name, query)
print(res)

####################################################################################################################################################################################################################################

# Create a Start Schema for analysis

# Create dim_date
table_name = "dim_date"
query = f"""
    WITH DATES_CTE AS (
    SELECT DISTINCT(O_ORDERDATE) AS date FROM ORDERS ORDER BY O_ORDERDATE
    )

    SELECT 
        strftime("%Y%m%d", date) AS date_skey
        , strftime("%Y", date) AS year
        , strftime("%m", date) AS month
        , strftime("%d", date) AS day
        , strftime("%m", date) AS month
    FROM
        DATES_CTE
        ;
    """
res = transform.transform_and_load_table(conformed_engine, semantic_engine, table_name, query)
print(res)

# Create dim_ship_instruction
table_name = "dim_ship_instruction"
query = f"""
    SELECT ROW_NUMBER() OVER (ORDER BY l_shipinstruct) AS l_shipinstruct_skey, l_shipinstruct FROM (SELECT DISTINCT l_shipinstruct FROM lineitem);
    """
res = transform.transform_and_load_table(conformed_engine, semantic_engine, table_name, query)
print(res)

# Create dim_ship_mode
table_name = "dim_ship_mode"
query = f"""
    SELECT ROW_NUMBER() OVER (ORDER BY l_shipmode) AS l_shipmode_skey, l_shipmode FROM (SELECT DISTINCT l_shipmode FROM lineitem);
    """
res = transform.transform_and_load_table(conformed_engine, semantic_engine, table_name, query)
print(res)

# Create dim_customer
table_name = "dim_customer"
query = f"""
    SELECT c_custkey, c_name, c_nationkey, c_acctbal_group FROM customer;
    """
res = transform.transform_and_load_table(conformed_engine, semantic_engine, table_name, query)
print(res)

#  create dim_part
table_name = "dim_part"
query = f"""
    SELECT p_partkey, p_name, p_type, p_retailprice FROM part;
    """
res = transform.transform_and_load_table(raw_engine, semantic_engine, table_name, query)
print(res)

# create dim_supplier
table_name = "dim_supplier"
query = f"""
    SELECT s_suppkey, s_name, s_nationkey, s_acctbal FROM supplier;
    """
res = transform.transform_and_load_table(raw_engine, semantic_engine, table_name, query)
print(res)

#create dim_nation
table_name = "dim_nation"
query = f"""
    SELECT A.n_nationkey, A.n_name, B.r_name FROM nation A LEFT JOIN region B ON A.n_regionkey = B.r_regionkey;
    """
res = transform.transform_and_load_table(raw_engine, semantic_engine, table_name, query)
print(res)   

# create fact_lineitem
table_name = "fact_lineitem"
query = f"""
SELECT 
STRFTIME('%Y%m%d', B.o_orderdate) AS order_date_skey
,B.o_custkey AS custkey
,A.l_partkey AS partkey
,A.l_suppkey AS suppkey
,A.l_returnflag AS returnflag
,A.l_linestatus AS linestatus
,A.l_shipinstruct AS shipinstruct
,A.l_shipmode AS shipmode
,SUM(l_quantity) AS quantity
,SUM(l_extendedprice) AS extendedprice
,l_discount AS discount
,l_tax AS tax
,SUM(l_gross_revenue) AS gross_revenue
,SUM(l_net_revenue) AS net_revenue

FROM lineitem A
LEFT JOIN orders B 
ON A.l_orderkey = B.o_orderkey

GROUP BY
STRFTIME('%Y%m%d', B.o_orderdate) 
,B.o_custkey 
,A.l_partkey
,A.l_suppkey 
,A.l_returnflag 
,A.l_linestatus
,A.l_shipinstruct
,A.l_shipmode
,l_discount
,l_tax
ORDER BY 
STRFTIME('%Y%m%d', B.o_orderdate) 
,B.o_custkey 
,A.l_partkey
,A.l_suppkey 
,A.l_returnflag 
,A.l_linestatus
,A.l_shipinstruct
,A.l_shipmode
,l_discount
,l_tax
;
"""
res = transform.transform_and_load_table(conformed_engine, semantic_engine, table_name, query)
print(res)  
