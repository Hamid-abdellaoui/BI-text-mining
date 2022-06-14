# + ------------------------ +
# |      SCRAP NEWS DATA     |
# + ------------------------ +
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
from datetime import datetime, timedelta


# Challenge.ma scraper
def scrap_challenge(days=30,nbr_pages=20, **kwargs):
    Data = []
    # to get the Data
    for url in  ['https://www.challenge.ma/category/economie/page/'+str(k) for k in range(1,nbr_pages)]:
        r = requests.get(url)
        print(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        box = soup.find(id='vw-page-content')
        articles = box.find_all(class_='vw-block-grid-item') 
        
        # for each article we scrap it's information
        for article in articles:
            date = article.find('time')['datetime']
            
            # transfome date from string to datetime
            date = date.split('T')[0]
            date = pd.to_datetime(date)
            last_ten_days = datetime.now() - timedelta(days=days)
            
            # check the date condition to scrap only specified nbr of days
            if date >= last_ten_days:
                h3 = article.find('h3', class_='vw-post-box-title')
                titre = h3.get_text()
                titre = " ".join(titre.split())
                img = article.find('img')['src']
                link = h3.find('a', href=True)['href'] 
                
                # scrap each link and get a text
                try:
                    r = requests.get(link)
                    soup = BeautifulSoup(r.content, 'html.parser')
                    article = soup.find(class_='vw-post-content').get_text()
                    text = " ".join(article.split())
                except:
                    text =''

                # filling the Data table by appending rows
                if text and date and link and titre:
                    Data.append([str(titre), text, date, link, img])
            else:
                break
            
    # save Data into a dataframe and intotaly save it into a csv file in the Raw data folder
    df = pd.DataFrame(Data, columns=[titre, text, date, link, img])
    df.to_csv('data/challenge.csv',index=False, header=['titre','text','date','link','img'], sep=',')
    kwargs['ti'].xcom_push(key='raw_scrapped', value='data/challenge.csv')
   
 
# La Vie Eco scraper    
def scrap_lavieeco(days=30,nbr_pages=10, **kwargs):
    Data = [] 
    for url in ['https://www.lavieeco.com/economie/page/'+str(k) for k in range(1,nbr_pages)]:
                
        r = requests.get(url)
        print(url)
        soup = BeautifulSoup(r.content, 'html.parser')

        box = soup.find('div',class_='listing')
        articles = box.find_all('article') 

        for article in articles:
            title = article.find('h2', class_='title')
            titre = title.get_text()
            titre = " ".join(titre.split())
            link = title.find('a', href=True)['href']
            try:
                r = requests.get(link)
                soup = BeautifulSoup(r.content, 'html.parser')
                article = soup.find(class_='entry-content').get_text()
                # print(article)
                date = soup.find('time', class_='post-published')['datetime']
                
                date = date.split('T')[0]
                
                date = pd.to_datetime(date)
                img = soup.find('div', class_='single-featured').find('img')['data-src']
                text, date = " ".join(article.split()), date
            except:
                img, text, date ='', '', datetime.now() 
            
            last_ten_days = datetime.now() - timedelta(days=days)
            
            if date >= last_ten_days:
                if text and date and link and titre:
                    Data.append([titre, text, date, link, img])
            else:
                break    
        # save Data into a dataframe and intotaly save it into a csv file in the Raw data folder
    df = pd.DataFrame(Data, columns=[titre, text, date, link, img])
    df.to_csv('data/lavieeco.csv',index=False, header=['titre','text','date','link','img'], sep=',')
    kwargs['ti'].xcom_push(key='raw_scrapped', value='data/lavieeco.csv')        


# Le matin scraper
def scrap_lematin(days=30,nbr_pages=16, **kwargs):
    Data = []
    for url in ['https://lematin.ma/journal/economie/'+str(k) for k in range(1,nbr_pages)]:
        r = requests.get(url)
        print(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        box = soup.find(class_='mx-1')
        articles = box.find_all('div',class_='card') 
        
        for article in articles:
            link = article.find('a', href=True)['href'] 
            titre = article.find('h4').get_text()
            titre = " ".join(titre.split())
            img = article.find('img', class_='card-img-top')['src']
            try:
                r = requests.get(link)
                soup = BeautifulSoup(r.content, 'html.parser')
                article = soup.find(class_='card-body').get_text()
                p = soup.find('p', class_='author')
                date = p.find_all('meta')[1]['content']
                
                # transfome date from string to datetime
                date = date.split(' ')[0]
                
                date = pd.to_datetime(date)
                text, date = " ".join(article.split()), date
            except:
                text, date ='', datetime.now() 
            last_ten_days = datetime.now() - timedelta(days=days)
            if date >= last_ten_days:
                if text and date and link and titre:
                    Data.append([titre, text, date, link, img])
            else:
                break    
        # save Data into a dataframe 
    df = pd.DataFrame(Data, columns=[titre, text, date, link, img])
    df.to_csv('data/lematin.csv',index=False, header=['titre','text','date','link','img'], sep=',')
    kwargs['ti'].xcom_push(key='raw_scrapped', value='data/lematin.csv')