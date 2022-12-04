import sys
import pandas as pd
from sqlalchemy import create_engine

if sys.argv[1] == "data_gen":
    raw_engine = create_engine('sqlite:///TPCH-sqlite/TPC-H.db', echo = False)    
else:
    raw_engine = create_engine('sqlite:///raw.db', echo = False)

semantic_engine = create_engine('sqlite:///semantic.db', echo = False)

raw_conn = raw_engine.connect()
semantic_conn = semantic_engine.connect()
# TESTING TOTALS REMAIN THE SAME FROM RAW TO SEMANTIC

# TEST QUANTITY
source_query = f"""
SELECT SUM(l_quantity) AS sum_quantity FROM lineitem
"""
df = pd.read_sql(source_query, raw_conn)
source_quantity = df.iloc[0]['sum_quantity']

target_query = f"""
SELECT SUM(quantity) AS sum_quantity FROM fact_lineitem
"""
df = pd.read_sql(target_query, semantic_conn)
target_quantity = df.iloc[0]['sum_quantity']

assert(source_quantity == target_quantity)

# TEST EXTENDEDPRICE
source_query = f"""
SELECT SUM(l_extendedprice) AS sum_extendedprice FROM lineitem
"""
df = pd.read_sql(source_query, raw_conn)
source_extendedprice = df.iloc[0]['sum_extendedprice']

target_query = f"""
SELECT SUM(extendedprice) AS sum_extendedprice FROM fact_lineitem
"""
df = pd.read_sql(target_query, semantic_conn)
target_extendedprice = df.iloc[0]['sum_extendedprice']
print(source_extendedprice, target_extendedprice)
assert(int(source_extendedprice) == int(target_extendedprice))

# TEST TOP 5 SUM Extended price by country
source_query = f"""
SELECT D.n_name AS name, SUM(A.l_extendedprice) AS sum_extendedprice 
FROM lineitem A
LEFT JOIN orders B ON A.l_orderkey = B.o_orderkey
LEFT JOIN customer C ON C.c_custkey = B.o_custkey
LEFT JOIN nation D ON D.n_nationkey = C.c_nationkey
GROUP BY  D.n_name
ORDER BY SUM(l_extendedprice) DESC
LIMIT 5
"""
df_source = pd.read_sql(source_query, raw_conn)

target_query = f"""
SELECT D.n_name AS name, SUM(A.extendedprice) AS sum_extendedprice 
FROM fact_lineitem A
LEFT JOIN dim_customer C ON C.c_custkey = A.custkey
LEFT JOIN dim_nation D ON D.n_nationkey = C.c_nationkey
GROUP BY  D.n_name
ORDER BY SUM(extendedprice) DESC
LIMIT 5
"""
df_target = pd.read_sql(target_query, semantic_conn)

assert(df_source.iloc[0]['name'] == df_target.iloc[0]['name'])
assert(df_source.iloc[1]['name'] == df_target.iloc[1]['name'])
assert(df_source.iloc[2]['name'] == df_target.iloc[2]['name'])
assert(df_source.iloc[3]['name'] == df_target.iloc[3]['name'])
assert(df_source.iloc[4]['name'] == df_target.iloc[4]['name'])

# TEST
print("All Tests Passed !!")
#..... more tests


