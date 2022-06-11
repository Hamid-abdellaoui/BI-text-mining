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
    
    box = soup.find('div',class_='listing')
    articles = box.find_all('article') 
    
    for article in articles:
        title = article.find('h2', class_='title')
        link = title.find('a', href=True)['href']  
        text, date = scrap_page(link) 
        last_ten_days = datetime.now() - timedelta(days=30)
        
        if date >= last_ten_days:
            if text and date and link:
                Data.append([text, date, link])
        else:
            break


def scrap_page(link):
    try:
        r = requests.get(link)
        soup = BeautifulSoup(r.content, 'html.parser')
        article = soup.find(class_='entry-content').get_text()
        # print(article)
        date = soup.find('time', class_='post-published')['datetime']
        
        date = date.split('T')[0]
        
        date = pd.to_datetime(date)
        return " ".join(article.split()), date
    except:
        return ' ', datetime.now()


#list of urls to scrape
n = 10
urls = ['https://www.lavieeco.com/economie/page/'+str(k) for k in range(1,n)]

         
articles = []

# to get the Data
for url in urls:
    get_Data(url)
        
# save Data into a dataframe
pd.DataFrame(Data, columns=['text', 'date', 'link']).to_csv('2.csv', index=False)


    
