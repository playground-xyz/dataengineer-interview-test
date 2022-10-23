---Customer_deatils query---------
Select C.*,N.N_NAME,R.R_NAME
from `PlayX.CUSTOMER` C
inner join `PlayX.NATION` N on C.C_NATIONKEY=N.N_NATIONKEY
inner join `PlayX.REGION` R on N.N_REGIONKEY=R.R_REGIONKEY;

---Part_deatils query---------
SELECT P.*,PS.*
From `PlayX.PART` P
inner join `PlayX.PARTSUPP` PS on P.P_PARTKEY=PS.PS_PARTKEY;

---Order_deatils query---------
SELECT O.*,L.*
From `PlayX.ORDERS` O
inner join `PlayX.LINEITEM` L on O.O_ORDERKEY=L.L_ORDERKEY;