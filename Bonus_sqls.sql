---add a fields to classify the customer account balance in 3 groups
Select *, 
Case 
when C_ACCTBAL < 0 then 'Below Average'
when C_ACCTBAL between 1 and 5000 then 'Average'
When C_ACCTBAL > 5000 then 'Above Average'
END ACCTBAL_Classification
From `clever-bee-240704.PlayX.CUSTOMER`

---add revenue per line item
select *,(L_EXTENDEDPRICE-(L_EXTENDEDPRICE*L_DISCOUNT)) +((L_EXTENDEDPRICE-(L_EXTENDEDPRICE*L_DISCOUNT))*L_TAX) revenue_per_lineitem
from `PlayX.LINEITEM` L;