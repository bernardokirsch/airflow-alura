import sys
sys.path.append('')

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join
from airflow.utils.dates import days_ago

with DAG(dag_id = "TwitterDag", start_date=days_ago(6), schedule_interval="@daily") as dag:
        
    TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'
    
    start_time = '{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}'
    end_time = '{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}'

    query = 'datascience'

    to = TwitterOperator(file_path=join(f'datalake/twitter_{query}',
                                        'extract_date={{ ds }}',
                                        'datascience_{{ ds_nodash }}.json'), 
                                        query=query, start_time=start_time, end_time=end_time, task_id='twitter_datascience')