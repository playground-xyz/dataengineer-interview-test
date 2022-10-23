-- 1. What are the top 5 nations in terms of revenue?

select Nation from 
(select N.N_NAME NATION,ROUND(Sum(O_TOTALPRICE),2) TOT_REVENUE from
--O_ORDERKEY,O_CUSTKEY,C.C_NATIONKEY,N.N_NATIONKEY,N.N_NAME,O_TOTALPRICE  
`PlayX.ORDERS` O 
inner join `PlayX.CUSTOMER` C on O.O_CUSTKEY=C.C_CUSTKEY
inner join `PlayX.NATION` N on C.C_NATIONKEY=N.N_NATIONKEY
group by 1 
order by 2 desc
limit 5);


-- 2. From the top 5 nations, what is the most common shipping mode?
with T1 as 
(select Nation from 
(select N.N_NAME NATION,ROUND(Sum(O_TOTALPRICE),2) TOT_REVENUE from
--O_ORDERKEY,O_CUSTKEY,C.C_NATIONKEY,N.N_NATIONKEY,N.N_NAME,O_TOTALPRICE  
`PlayX.ORDERS` O 
inner join `PlayX.CUSTOMER` C on O.O_CUSTKEY=C.C_CUSTKEY
inner join `PlayX.NATION` N on C.C_NATIONKEY=N.N_NATIONKEY
group by 1 
order by 2 desc
limit 5))
Select T2.NATION,T2.L_SHIPMODE from 
(select NATION,L_SHIPMODE
from (
select N.N_NAME NATION,L.L_SHIPMODE,count(*) SHIPMODE_COUNT from
`PlayX.ORDERS` O 
inner join `PlayX.CUSTOMER` C on O.O_CUSTKEY=C.C_CUSTKEY
inner join `PlayX.NATION` N on C.C_NATIONKEY=N.N_NATIONKEY
inner join `PlayX.LINEITEM` L on O.O_ORDERKEY=L.L_ORDERKEY
group by 1,2 
order by 3 desc)
QUALIFY RANK() OVER (PARTITION BY NATION ORDER BY SHIPMODE_COUNT DESC) = 1)T2,T1
where T2.NATION=T1.NATION;

-- 3. What are the top selling months?

Select TOP_SELLING_MONTHS from 
(select FORMAT_DATE('%B', O_ORDERDATE) TOP_SELLING_MONTHS,ROUND(Sum(O_TOTALPRICE),2)
 from `PlayX.ORDERS`
group by 1 order by 2 desc
limit 3);

-- 4. Who are the top customer in terms of revenue and/or quantity?

Select C.C_NAME,ROUND(Sum(O.O_TOTALPRICE),2) TOT_REVENUE
from  `PlayX.ORDERS` O inner join
`PlayX.CUSTOMER` C on O.O_CUSTKEY=C.C_CUSTKEY
group by 1 order by 2 desc
limit 5;

-- 5. Compare the sales revenue of on current period against previous period?
--by year
select Year, ROUND(100*(TOT_REVENUE-LAG(TOT_REVENUE) OVER ( ORDER BY Year ))/LAG(TOT_REVENUE) OVER ( ORDER BY Year ),2) YoY_pct_growth from
(Select EXTRACT(YEAR from O.O_ORDERDATE) Year, ROUND(Sum(O.O_TOTALPRICE),2) TOT_REVENUE
from `PlayX.ORDERS` O 
group by 1 order by 1)
where year != 1998 -- ignoring 1998 as that's the current running year
order by 1 
--by month
select Year,month, ROUND(100*(TOT_REVENUE-LAG(TOT_REVENUE) OVER ( ORDER BY Year,month ))/LAG(TOT_REVENUE) OVER ( ORDER BY Year,month ),2) MoM_pct_growth from
(Select EXTRACT(YEAR from O.O_ORDERDATE) Year,EXTRACT(MONTH from O.O_ORDERDATE) month, ROUND(Sum(O.O_TOTALPRICE),2) TOT_REVENUE
from `PlayX.ORDERS` O 
group by 1,2 order by 1,2)
where year != 1998 and month != 8 -- ignoring 1998's 8th month as that's the current running month and incomplete
order by 1,2 