# Data Engineer Interview Test

We are looking for a high quality data engineer which can deliver comprehensive solutions for our continuity and business growth. 

Our amazing team drives data culture, we want to change how we produce data from large batches to micro batching, from daily to near real-time/streaming processing, from tabular reports to insightful dashboards.  

You can be part of this amazing team which deals with data all the time using different process, tools and technologies.

Following is a little treasure and challenge for those keen on joining this wonderful company and team.

## Junior/Mid 
For a Junior/Mid role we are expecting at least 2-3 tables to be loaded and an aggregated report done.

## Senior
We are expecting the most from you.

# The Project
Build a small ETL process to digest a few set of files into a data warehouse like project. 

We are expecting an end-to-end ETL solution to deliver a simple star schema which an end user can easily slice and dice the data through a report or using basic ad-hoc query.

As any project, time could be a constraint, make it simple to not spend too much time on it, ideally candidates spend from 1h to few hours depending on your style of work and knowledge of data engineer. 

### Tools and Technologies
We are a Python and SQL workshop, we would like to see this project using just those tools.  

However, we are open to other tools and technologies if we are able to easily replicate on our side. 

For the database, use a simple and lightweight optimizer for your database, but don't be limited to it. 

Please, avoid licensed products, this might block and limit us to review your work. 

How to do it?
-----------------------
Fork/Copy this repo, build your data processing layer and follow the best practices in SDLC. Open a Pull Request and send us a message highlighting the test is completed.

#### Rules
* it must come with step by step instructions to run the code.
* please, be mindful that your code might be moved or deleted after we analyse the PR. 
* don't forget the best practices
* be able to explain from the ground up the whole process on face to face interview
* use your time wisely

The small ETL project
--------- 

1. The data for this exercise can be found on the `data.zip` file. Can you describe the file format?

**Super Bonus**: generate your own data through the instructions on the encoded file `bonus_etl_data_gen.txt`.
To get the bonus points, please encoded the file with the instructions were used to generate the files.

2. Code you scripts to load the data into a database.

3. Design a star schema model which the data should flow.

4. Build your process to load the data into the star schema 

**Bonus** point: 
- add a fields to classify the customer account balance in 3 groups 
- add revenue per line item 
- convert the dates to be distributed over the last 2 years

5. How to schedule this process to run multiple times per day?
 
**Bonus**: What to do if the data arrives in random order and times via streaming?

6. How to deploy this code?

**Bonus**: Can you make it to run on a container like process (Docker)? 

Data Reporting
-------
One of the most important aspects to build a DWH is to deliver insights to end-users. 

Can you using the designed star schema (or if you prefer the raw data), generate SQL statements to answer the following questions:

1. What are the top 5 nations in terms of revenue?

2. From the top 5 nations, what is the most common shipping mode?

3. What are the top selling months?

4. Who are the top customer in terms of revenue and/or quantity?

5. Compare the sales revenue of on current period against previous period?


Data profilling
----   
Data profiling are bonus.

What tools or techniques you would use to profile the data?
 
What results of the data profiling can impact on your analysis and design?   



Architecture
-----
If this pipeline is to be build for a real live environment.
What would be your recommendations in terms of tools and process?

Would be a problem if the data from the source system is growing at 6.1-12.7% rate a month?



ERD
--
![alt text](erd.png "ERD")

Author: adilsonmendonca
