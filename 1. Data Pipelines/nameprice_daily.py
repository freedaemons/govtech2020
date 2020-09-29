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

# DAG-specific constants
SECRET_NAME = 'govtech2020-test'
REGION_NAME = 'us-east-1'
BUCKET_NAME = 'nameprice'
KEY = 'dataset.csv' 

def process_nameprice(**kwargs):
	
# retrieve data file
	s3_client = boto3.client('s3')

	try:
	    s3_client.download_file(BUCKET_NAME, KEY, 'data/dataset.csv')
	except botocore.exceptions.ClientError as e:
	    if e.response['Error']['Code'] == "404":
	        print("The object does not exist.")
	    else:
	        raise

	return 'End'


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