--DW Queries
--What are the top 5 nations in terms of revenue?
SELECT r.country, sum(o_totalprice) AS revenue FROM (
                  SELECT DISTINCT l_orderkey, o_totalprice, dc.country
                  FROM dw_lineitem
                           INNER JOIN dw_country dc ON dw_lineitem.n_nationkey = dc.n_nationkey
              ) r
GROUP BY r.country
ORDER BY revenue DESC
LIMIT 5
;

--From the top 5 nations, what is the most common shipping mode?
SELECT l_shipmode, count(l_shipmode) AS count FROM dw_lineitem
WHERE n_nationkey IN
      (
          SELECT r.n_nationkey
          FROM (
                   SELECT DISTINCT o_totalprice, dc.n_nationkey
                   FROM dw_lineitem
                            INNER JOIN dw_country dc ON dw_lineitem.n_nationkey = dc.n_nationkey
               ) r
          GROUP BY r.n_nationkey
          ORDER BY SUM(r.o_totalprice) DESC
          LIMIT 5
      )
GROUP BY l_shipmode
ORDER BY count DESC
;

--What are the top selling months?
SELECT to_char(o_orderdate, 'Month') AS Month, count(*) AS number_of_orders
FROM dw_orders
GROUP BY to_char(o_orderdate, 'Month')
ORDER BY number_of_orders DESC
;

--Who are the top customer in terms of revenue and/or quantity?
SELECT c.c_name, SUM(o_totalprice) AS revenue, SUM(l_quantity) AS quantity FROM dw_lineitem
INNER JOIN customer c ON dw_lineitem.c_custkey = c.c_custkey
GROUP BY c.c_name
ORDER BY revenue DESC, quantity DESC
;

--Compare the sales revenue of on current period against previous period?
SELECT date_part('year', o_orderdate), sum(o_totalprice) AS revenue FROM dw_orders
GROUP BY date_part('year', o_orderdate)
ORDER BY date_part('year', o_orderdate) DESC
LIMIT 2
;
