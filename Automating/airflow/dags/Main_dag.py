# Imports
import os
import pendulum
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from includes.Scraping_news import scrap_lematin, scrap_challenge, scrap_lavieeco
from airflow.utils.task_group import TaskGroup
import pandas as pd
import datetime
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator


# Set arguments
us_east_tz = pendulum.timezone('America/New_York')
default_args = {
    'owner': 'Hamid Abdellaoui',
    'start_date': datetime(2020, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'wait_for_downstream': True
}

# get all the files in the Raw data folder
files = [f for f in os.listdir('data/Raw') if os.path.isfile(os.path.join('data/Raw', f))]
# read each file and save it into a dataframe

def initial_processing():
    # read
    df = pd.concat([pd.read_csv('data/Raw/'+file) for file in files])
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df.to_csv('data/Dataset/data.csv',index=False, header=['titre','text','date','link','img'], sep=',')
    
# def text_mining_preprocessing(**kwargs):
#     df = pd.read_csv('data/Dataset/data.csv')
#     df['text'] = df['text'].apply(lambda x: x.replace('\n', ' '))
    
dag = DAG(
    dag_id='Data_pipeline_for_text_mining',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    description='Pipeline for scraping data from news websites and process them using text mining techniques',
    tags=['Moroccan Economy', 'scrapping', 'text-mining']
)

with dag:
    
    start = DummyOperator(task_id='start')

    with TaskGroup(group_id='scraping_data') as scraping_data:
        
        scrap_challenge = PythonOperator(
            task_id='scrap_challenge',
            python_callable=scrap_challenge,
            op_kwargs={'days': 60, 'nbr_pages':42},
            # provide_context=True
        )
        
        scrap_lavieeco = PythonOperator(
            task_id='scrap_lavieeco',
            python_callable=scrap_lavieeco,
            op_kwargs={'days': 60, 'nbr_pages':22}
            # provide_context=True
        )
          
        scrap_lematin = PythonOperator(
            task_id='scrap_lematin',
            python_callable=scrap_lematin,
            op_kwargs={'days': 60, 'nbr_pages':34}
            # provide_context=True
        )    
    
    initial_processing = PythonOperator(
        task_id='initial_processing',
        python_callable=initial_processing,
        wait_for_downstream = True,
        depends_on_past = True
        )
    
    # Start Task Group definition
    with TaskGroup(group_id='text_mining') as text_mining:
        t1 = DummyOperator(task_id='task1')
        t2 = DummyOperator(task_id='task2')

        t1 >> t2
    # End Task Group definition
    
    end = DummyOperator(task_id='end')
  
  
start >> scraping_data >> initial_processing >> text_mining >> end