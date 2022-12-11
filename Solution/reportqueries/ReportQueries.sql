--What are the top 5 nations in terms of revenue?
SELECT n.n_name AS Country , SUM(o_totalprice) AS Revenue FROM orders
INNER JOIN customer c ON orders.o_custkey = c.c_custkey
INNER JOIN nation n ON c.c_nationkey = n.n_nationkey
GROUP BY n_name
ORDER BY Revenue DESC
LIMIT 5;


--From the top 5 nations, what is the most common shipping mode?
SELECT l_shipmode, count(*) AS count FROM lineitem
INNER JOIN orders o ON lineitem.l_orderkey = o.o_orderkey
INNER JOIN customer c ON o.o_custkey = c.c_custkey
WHERE c_nationkey IN
(SELECT  n.n_nationkey FROM orders
INNER JOIN customer c ON orders.o_custkey = c.c_custkey
INNER JOIN nation n ON n.n_nationkey = c.c_nationkey
GROUP BY n.n_nationkey
ORDER BY SUM(o_totalprice) DESC
LIMIT 5
)
GROUP BY l_shipmode
ORDER BY count DESC
;

--What are the top selling months?
SELECT to_char(o_orderdate, 'Month') AS Month, count(*) AS number_of_orders
FROM orders
GROUP BY to_char(o_orderdate, 'Month')
ORDER BY number_of_orders DESC
;

--Who are the top customer in terms of revenue and/or quantity?
SELECT o_custkey, c_name, SUM(o_totalprice) AS totat_price, SUM(l_quantity) AS quantity
FROM orders
INNER JOIN lineitem l ON orders.o_orderkey = l.l_orderkey
INNER JOIN customer c ON c.c_custkey = orders.o_custkey
GROUP BY o_custkey, c_name
ORDER BY totat_price DESC, quantity DESC
;

--Compare the sales revenue of on current period against previous period?
SELECT date_part('year', o_orderdate), sum(o_totalprice) AS revenue FROM orders
GROUP BY date_part('year', o_orderdate)
ORDER BY date_part('year', o_orderdate) DESC
LIMIT 2
;