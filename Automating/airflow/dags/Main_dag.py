# Imports
import os
import pendulum
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from includes.Scraping_news import scrap_lematin, scrap_challenge, scrap_lavieeco
from includes.text_mining import *
from includes.text_mining import data_frame_bigrams
from airflow.utils.task_group import TaskGroup
import pandas as pd
import datetime
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
np.set_printoptions(precision=2, linewidth=80)
from nltk import FreqDist

# Gensim
import gensim
from gensim import corpora
from gensim.models.coherencemodel import CoherenceModel

import spacy
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk import ngrams
import re
#from bs4 import BeautifulSoup
import unicodedata

from spacy.lang.fr.stop_words import STOP_WORDS

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer

import fr_core_news_md #import spacy french stemmer
from sklearn.decomposition import NMF





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
    df['period'] = df['date'].to_period()
    df.to_csv('data/Dataset/data.csv',index=False, header=['titre','text','date','link','img'], sep=',')
    
def corpuses_NMF ():
    dataset = pd.read_csv('data/Dataset/data.csv')
    print(type(dataset['date']))
    print(dataset.info())
    corpus = dataset.text.values.tolist()
    corpuses_NMF = []
    l_periods=list(set(dataset.period.values))
    l_periods.sort()
    for i in range(len(l_periods)-1) :
        indice1=dataset.period.values.tolist().index(l_periods[i])
        indice2=dataset.period.values.tolist().index(l_periods[i+1])
        liste = []
        for j in range(indice1,indice2,1):
            liste.append(corpus[j]) 
        corpuses_NMF.append(liste)

    dernier_indice=dataset.period.values.tolist().index(l_periods[1])
    derniere_corpus = [] 
    for j in range(dernier_indice,len(dataset),1):
        derniere_corpus.append(corpus[j]) 
    corpuses_NMF.append(derniere_corpus)
    
    outputs =[]
    for i in range(len(corpuses_NMF)): 
        outputs.append(process_corpus(corpuses_NMF[i],i))
    
    print(type(outputs))
    print(outputs)
    # transform to json and push to next task (databases)
    # return outputs
    
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
        
        # scrap_challenge = PythonOperator(
        #     task_id='scrap_challenge',
        #     python_callable=scrap_challenge,
        #     op_kwargs={'days': 60, 'nbr_pages':42},
        #     # provide_context=True
        # )
        
        # scrap_lavieeco = PythonOperator(
        #     task_id='scrap_lavieeco',
        #     python_callable=scrap_lavieeco,
        #     op_kwargs={'days': 60, 'nbr_pages':22}
        #     # provide_context=True
        # )
          
        # scrap_lematin = PythonOperator(
        #     task_id='scrap_lematin',
        #     python_callable=scrap_lematin,
        #     op_kwargs={'days': 60, 'nbr_pages':34}
        #     # provide_context=True
        # )    
        t1 = DummyOperator(task_id='t1')
        t2 = DummyOperator(task_id='t2')
        t3 = DummyOperator(task_id='t3')
  
    
    initial_processing = PythonOperator(
        task_id='initial_processing',
        python_callable=initial_processing,
        wait_for_downstream = True,
        depends_on_past = True
        )
    
    # Start Task Group definition
    with TaskGroup(group_id='text_mining') as text_mining:
        corpuses_NMF = PythonOperator(
            task_id='corpuses_NMF',
            python_callable=corpuses_NMF,
            # provide_context=True
        )
        data_frame_bigrams = PythonOperator(
            task_id='data_frame_bigrams',
            python_callable=data_frame_bigrams,
            
            )
        [data_frame_bigrams , corpuses_NMF]
    # End Task Group definition
    
    end = DummyOperator(task_id='end')
  
  
start >> scraping_data >> initial_processing >> text_mining >> end