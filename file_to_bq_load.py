from google.cloud import bigquery
import sys

##(sys.argv[0]) - Tablename and (sys.argv[1]) - Filename needs to be passed when running the script
##ex: Python file_to_bq_load.py CUSTOMER customer.tbl

# upload to BigQuery
client = bigquery.Client(project="clever-bee-240704")

table_ref = client.dataset("PlayX").table(sys.argv[1])

job_config = bigquery.LoadJobConfig()
job_config.allow_quoted_newlines = True
job_config.field_delimiter="|"
job_config.source_format = bigquery.SourceFormat.CSV
# job_config.skip_leading_rows = 1 # ignore the header
#job_config.autodetect = 

with open(sys.argv[2], "rb") as source_file:
        job = client.load_table_from_file(
                        source_file, table_ref, job_config=job_config
                            )

        # job is async operation so we have to wait for it to finish
        job.result()
