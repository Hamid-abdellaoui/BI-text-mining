import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
from datetime import datetime, timedelta


Data = [] 

def scrap_page(link):
    try:
        r = requests.get(link)
        soup = BeautifulSoup(r.content, 'html.parser')
        article = soup.find(class_='vw-post-content').get_text()
        return " ".join(article.split())
    except:
        return ''


def get_Data(url):
    r = requests.get(url)
    print(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    
    box = soup.find(id='vw-page-content')
    articles = box.find_all(class_='vw-block-grid-item') 
    
    for article in articles:
        date = article.find('time')['datetime']
        # transfome date from string to datetime
        date = date.split('T')[0]
        date = pd.to_datetime(date)
        last_ten_days = datetime.now() - timedelta(days=10)
        if date >= last_ten_days:
            h3 = article.find('h3', class_='vw-post-box-title')
            link = h3.find('a', href=True)['href'] 
            text = scrap_page(link) 
            if text and date and link:
                Data.append([text, date, link])
        else:
            break


#list of urls to scrape
n = 2
urls = ['https://www.challenge.ma/category/economie/page/'+str(k) for k in range(1,n)]

         


# to get the Data
for url in urls:
    get_Data(url)
        
# save Data into a dataframe
pd.DataFrame(Data, columns=['text', 'date', 'link']).to_csv('1.csv', index=False)
