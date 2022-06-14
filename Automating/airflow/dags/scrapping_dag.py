# Imports
import pendulum
import os
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from includes.Scraping_news import scrap_challenge, scrap_lavieeco, scrap_lematin

import pandas as pd
import datetime
from datetime import datetime, timedelta

# Set arguments

us_east_tz = pendulum.timezone('America/New_York')
default_args = {
    'owner': 'Hamid Abdellaoui',
    'start_date': datetime(2022, 1, 7, 7, 30, tzinfo=us_east_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
import json

def initial_processing(**kwargs):
    data_paths = kwargs['ti'].xcom_pull(key='raw_scrapped', task_ids=['scrap_challenge','scrap_lavieeco','scrap_lematin'])
    df = pd.concat([pd.read_csv(data_paths[0]), pd.read_csv(data_paths[1]), pd.read_csv(data_paths[2])])
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.to_csv('data/Dataset/data.csv',index=False, header=['titre','text','date','link','img'], sep=',')
    
def text_mining_preprocessing(**kwargs):
    df = pd.read_csv('data/Dataset/data.csv')
    df['text'] = df['text'].apply(lambda x: x.replace('\n', ' '))
    
dag = DAG(
    dag_id='scrapping',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
)

with dag:
    scrap_challenge = PythonOperator(
        task_id='scrap_challenge',
        python_callable=scrap_challenge,
        # op_kwargs={'days': 10, 'nbr_pages':4},
        # provide_context=True
    )
    
    scrap_lavieeco = PythonOperator(
        task_id='scrap_lavieeco',
        python_callable=scrap_lavieeco,
        # op_kwargs={'days': 10, 'nbr_pages':2}
        # provide_context=True
    )
    
    scrap_lematin = PythonOperator(
        task_id='scrap_lematin',
        python_callable=scrap_lematin,
        # op_kwargs={'days': 10, 'nbr_pages':2}
        # provide_context=True
    )    
    
    initial_processing = PythonOperator(
            task_id='initial_processing',
            python_callable=initial_processing
        )


[scrap_challenge, scrap_lavieeco, scrap_lematin] >> initial_processing