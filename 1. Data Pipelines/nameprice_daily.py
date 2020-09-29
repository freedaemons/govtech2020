# Libraries
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.settings import pendulum

import boto3
import base64
from botocore.exceptions import ClientError

import os
import pandas as pd
import re

# DAG-specific constants
SECRET_NAME = 'govtech2020-test'
REGION_NAME = 'us-east-1'
IN_BUCKET_NAME = 'nameprice'
IN_KEY = 'dataset.csv' 
OUT_BUCKET_NAME = 'clean_nameprice'
OUT_KEY = 'clean_dataset.csv'
IN_FILEPATH = os.path.join('data', 'dataset.csv')
OUT_FILEPATH = os.path.join('output', 'result.csv')

# main task callable
def process_nameprice(**kwargs):
# retrieve data file
	s3_client = boto3.client('s3')

	try:
	    s3_client.download_file(IN_BUCKET_NAME, IN_KEY, IN_FILEPATH)
	except botocore.exceptions.ClientError as e:
	    if e.response['Error']['Code'] == "404":
	        print("The object does not exist.")
	    else:
	        raise

# process file and write to output/result.csv
	result_summary = clean_nameprice(datafile)

	s3_client.upload_file(OUT_FILEPATH, OUT_BUCKET_NAME, OUT_KEY)

	return result_summary

# data cleaning logic
def clean_nameprice(datafile):
	df = pd.read_csv(IN_FILEPATH)
	in_shape = df.shape

	outdf = df.copy()
# take last 2 space-delimited tokens as first and last name. assumes name field has at least 2 space-delimited tokens in string
	outdf['firstname'] = outdf['name'].apply(lambda x: re.split(' ',x)[-2])
	outdf['lastname'] = outdf['name'].apply(lambda x: re.split(' ',x)[-1])
# pandas automatically reads numeric column, removing prepended 0s
# remove rows with no name
	outdf = outdf.loc[outdf['name'] != '']
# flag if price > 100
	outdf['above_100'] = outdf['price'].apply(lambda x: 'true' if x > 100 else 'false')

	outdf = outdf[['firstname', 'lastname', 'price', 'above_100']]
# write to output file
	out_df.to_csv(OUT_FILEPATH, index=False)

	outshape = outdf.shape

# log diff in records between infile and outfile
	result = {
		'inshape': inshape
		'outshape': outshape
	}

	return result


# DAG boilerplate
default_args={
    # Set your alias here so we know who to contact when fires start
    'owner': 'friedan',
    # Leave it as this unless you know what you are doing (backfill)
    'start_date': days_ago(1),
    # If the run fails, we can try this number of times more after retry_delay
    'retries': 0,
    # Set delay as 5 minutes in case our resource is busy doing something else
    'retry_delay': timedelta(minutes=5),
    # Send email(s) when the DAG experiences an error
    'email': ['friedemann.ang@gmail.com'],
    'email_on_failure': True,
    # This is used to prevent backfill. Leave unless you know what you are doing
    'catchup': False,
    'depends_on_past': False
}

# Configure the DAG here
with DAG(
    dag_id='daily_process_nameprice',
    default_args=default_args,
    # cron-like syntax or helpers as defined in dqlib.INTERVALS
    schedule_interval='@daily',
    # This is used to prevent backfill. Leave unless you know what you are doing
    catchup=False
) as dag:

    daily_process_nameprice_task = PythonOperator(
        # The task ID is what appears in the graph/tree view
        task_id="process_nameprice",
        # Set this to TRUE or else we cannot access ti object
        provide_context=True,
        # This is like your main() method
        python_callable=process_nameprice,
        dag=dag
    )