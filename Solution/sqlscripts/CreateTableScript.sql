/* Create table script to implement star schema dimensional model */
CREATE TABLE dw_country
(
    c_id SERIAL PRIMARY KEY,
    n_nationkey INT NOT NULL ,
    country text,
    region text,
    etl_timestamp TIMESTAMP DEFAULT NOW()
);

CREATE TABLE dw_customer
(
    c_id SERIAL PRIMARY KEY ,
    c_custkey INT NOT NULL ,
    c_nationkey INT NOT NULL ,
    c_name TEXT,
    c_address TEXT,
    c_acctbal INT,
    etl_timestamp TIMESTAMP DEFAULT NOW()
);

CREATE TABLE dw_lineitem
(
    l_id          SERIAL PRIMARY KEY,
    l_orderkey    INT NOT NULL,
    c_custkey     INT NOT NULL,
    n_nationkey   INT NOT NULL,
    l_linenumber  INT NOT NULL,
    l_quantity    INT NOT NULL,
    o_totalprice  INT NOT NULL,
    l_shipmode    TEXT,
    etl_timestamp TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX dw_lineitem_ocnl ON playground.dw_lineitem(l_orderkey, c_custkey, n_nationkey, l_linenumber);


CREATE TABLE dw_orders
(
    o_id SERIAL PRIMARY KEY ,
    o_orderkey INT NOT NULL ,
    o_custkey INT NOT NULL ,
    o_totalprice INT NOT NULL ,
    o_orderdate DATE NOT NULL ,
    etl_timestamp TIMESTAMP DEFAULT NOW()

);
CREATE UNIQUE INDEX dw_order_custkey ON playground.dw_orders(o_orderkey, o_custkey);

