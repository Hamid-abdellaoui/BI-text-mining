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
        
        if len(articles)>0:
            for article in articles:
                link = article.find('a', href=True)['href'] 
                titre = article.find('h3').get_text()
                titre = " ".join(titre.split())

                try:
                    r = requests.get(link)
                    soup = BeautifulSoup(r.content, 'html.parser')
                    article = soup.find(class_='vw-post-content').get_text()
                    p = soup.find('p', class_='vw-post-meta-author')
                    img = soup.find('img',itemprop="image")['src']
                    date = p.find('meta', itemprop='datePublished')['content']
 
                    # transfome date from string to datetime
                    date = date.split(' ')[0]
                    date = pd.to_datetime(date)
                    text, date = " ".join(article.split()), date
                except:
                    img, text, date ='','', datetime.now() 
                
                last_ten_days = datetime.now() - timedelta(days=days)
                if date >= last_ten_days:
                    if text and date and link and titre:
                        Data.append([titre, text, date, link, img])
                else:
                    break
        else:
            continue
    # save Data into a dataframe and intotaly save it into a csv file in the Raw data folder
    df = pd.DataFrame(Data, columns=[titre, text, date, link, img])
    df.to_csv('data/Raw/challenge.csv',index=False, header=['titre','text','date','link','img'], sep=',')
   

# La Vie Eco scraper    
def scrap_lavieeco(days=30,nbr_pages=10):
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
    df.to_csv('data/Raw/lavieeco.csv',index=False, header=['titre','text','date','link','img'], sep=',')
      


# Le matin scraper
def scrap_lematin(days=30,nbr_pages=16):
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

            try:
                r = requests.get(link)
                soup = BeautifulSoup(r.content, 'html.parser')
                article = soup.find(class_='card-body').get_text()
                p = soup.find('p', class_='author')
                img = soup.find('img',itemprop="image")['src']
                date = p.find('meta', itemprop='datePublished')['content']
 
                # transfome date from string to datetime
                date = date.split(' ')[0]
                date = pd.to_datetime(date)
                text, date = " ".join(article.split()), date
            except:
                img, text, date ='','', datetime.now() 
            
            last_ten_days = datetime.now() - timedelta(days=days)
            if date >= last_ten_days:
                if text and date and link and titre:
                    Data.append([titre, text, date, link, img])
            else:
                break    
        # save Data into a dataframe 
    df = pd.DataFrame(Data, columns=[titre, text, date, link, img])
    df.to_csv('data/Raw/lematin.csv',index=False, header=['titre','text','date','link','img'], sep=',')