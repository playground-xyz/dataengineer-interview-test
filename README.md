## C. The old fashion ETL Master 

1. The data for this exercise can be found on the `data.zip` file. Can you describe the file format?
 - .TBL's are a table like format files.

**Super Bonus**: generate your own data through the instructions on the encoded file `bonus_etl_data_gen.txt`.
To get the bonus points, please encoded the file with the instructions were used to generate the files.
 - I have placed the decoded file in the repo. But i have not used this new data source.

2. Code you scripts to load the data into a database.
   - Attached file_to_bq_load.py scirpt was used to load the data from files into Biquery tables. 

3. Design a star schema model which the data should flow.
 - ![Start_schema](https://user-images.githubusercontent.com/92492781/197368531-1e79b7c4-0b23-4868-a202-396433609855.PNG)

4. Build your process to load the data into the star schema
 -  Attached file_to_bq_load.py scirpt was used to load the data from files into Biquery tables.
 -  Sql script to load star schema tables from the Raw tables are updated to the files - "SQL to load start_schema".

**Bonus** point: 
- add a fields to classify the customer account balance in 3 groups 
- add revenue per line item 
- convert the dates to be distributed over the last 2 years
    - I have added an SQL file called as Bonus_sql for the first two points given here.
    - On the 3rd one, can't really understand the requirement.

**Bonus**: What to do if the data arrives in random order and times via streaming?
  - Data files to be dropped into GCS bucket ----> CloudFucntion ----> Bigquery (Near real time)
  - Data to be added to PubSub topic ----> Cloud DataFlow ----> Bigquery(Realtime streaming)

**Bonus**: Would be a problem if the data from the source system is growing at 6.1-12.7% rate a month?
   - Not in GCP. As we are used serverless frameworks, the increase in the data volume should be handled automatically.
   - But to improve the query/DB performance, we have to build the tables with partiotions and clusters.
   - Purging of old data might be required if in case they aren't requried.

### Data Reporting
One of the most important aspects to build a DWH is to deliver insights to end-users. 

Can you using the designed star schema (or if you prefer the raw data), generate SQL statements to answer the following questions:

1. What are the top 5 nations in terms of revenue?

2. From the top 5 nations, what is the most common shipping mode?

3. What are the top selling months?

4. Who are the top customer in terms of revenue and/or quantity?

5. Compare the sales revenue of on current period against previous period?

 - SQL's for the same has been added to the files "SQLs for Data Reporting"
---

Data profilling
----   
Data profiling are bonus.

What tools or techniques you would use to profile the data?
 -  In GCP we use DataPrep for profiling.
 -  But the most common tools used in the market are Informatica, Talend to name some.
 
What results of the data profiling can impact on your analysis and design?   
 - Profilfing is done to identify and highlight any discrepancies in the data.
 - Making use for data profiling will beneift the business in gaining data accuracy and correctness over the data thats being used for critical desicion making   purposes. 


ERD for the ETL Master option
--
![alt text](erd.png "ERD")

Author: adilsonmendonca


