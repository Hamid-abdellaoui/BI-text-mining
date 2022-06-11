import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
from datetime import datetime, timedelta

Data = [] 

def get_Data(url):
    r = requests.get(url)
    print(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    
    box = soup.find(class_='mx-1')
    articles = box.find_all('div',class_='card') 
    
    for article in articles:
        
        link = article.find('a', href=True)['href'] 
        
        text, date = scrap_page(link) 
        last_ten_days = datetime.now() - timedelta(days=10)
        
        if date >= last_ten_days:
            if text and date and link:
                Data.append([text, date, link])
        else:
            break


def scrap_page(link):
    try:
        r = requests.get(link)
        soup = BeautifulSoup(r.content, 'html.parser')
        article = soup.find(class_='card-body').get_text()
        p = soup.find('p', class_='author')
        date = p.find_all('meta')[1]['content']
        print(date)
        # transfome date from string to datetime
        date = date.split(' ')[0]
        print(type(date))
        date = pd.to_datetime(date)
        return " ".join(article.split()), date
    except:
        return ' ', datetime.now()


#list of urls to scrape
n = 6
urls = ['https://lematin.ma/journal/economie/'+str(k) for k in range(1,n)]

         
articles = []

# to get the Data
for url in urls:
    get_Data(url)
        
# save Data into a dataframe
pd.DataFrame(Data, columns=['text', 'date', 'link']).to_csv('2.csv', index=False)
