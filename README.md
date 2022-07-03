# BI-text-mining

An academic project that aims to extract and process text from a large amount of articles scrapped from many Moroccan news Websites.

This project is divided into 5 parts, each part is in an independent directory:




- **[Part 1]**: **[Scraping](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Scraping/)**
- **[Part 2]**: **[Text_processing](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Text_processing/)**
- **[Part 3]**: **[Automating](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Automating/)**
- **[Part 4]**: **[Reporting](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Reporting/)**
- **[Part 5]**: **[Mining](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Mining/)**

<br>

# Further Details

## 1. **[Scraping](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Scraping/)** : 
Scrapping articles (Title, publiction date, Image, Link, Full text...)from Moroccan news websites(BeatifulSoup and requests).

### <b> Ressources : </b>

Data was retrieved from the following websites:
> [Le matin.ma](https://www.lematin.ma/) <br>
> [La vie eco](https://www.lavieeco.com) <br>
> [Challenge.ma](https://www.challenge.ma/) <br>

### <b> Data structure : </b>

> We scrapped the Economy subcategory pages for each news website. for each article we got its :
- `title` 
- `publication date`
- `image`
- `link`
- `full text`.

## 2. **[Text_processing](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Text_processing/)** : 
Apply some text mining methods and algorithms(TF,IDF, NMF, TOPIC MODELING).

* >Texts are pre-treated and cleaned using the basic text processing techniques such as :
    - `removing stop words`
    - `lemmatization`
    - `stemming`
    - `tokenization`
    - `removing punctuation`
    - `removing numbers`
    - `and`
    - `removing special characters`.
* > Then we've applied some text mining algorithms such as :
    - `TF-IDF` 
    - `NMF` 
    - `Topic Modeling`.

## 3. **[Automating](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Automating/)** : 
Automate the process of scraping, text processing, Datawarehousing and loading Data into Postgresql Database(Airflow, Docker...).
The Datapipeline architecture is as follows:

<!-- image -->
![](https://github.com/Hamid-abdellaoui/BI-text-mining/blob/master/Automating/data/Image1.png?raw=true)


## 4. **[Reporting](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Reporting/)** : 
    Present results and key measures in a dashboard (Web app with Flask).

Reporting results via a simple dashboard as follows:
<!-- IMAGE -->
![](https://raw.githubusercontent.com/Hamid-abdellaoui/BI-text-mining/master/Reporting/Dahboarding%20App/static/images/Image1.png)


## 5. **[Mining](https://github.com/Hamid-abdellaoui/BI-text-mining/tree/master/Mining/)** : 
Extract association rules (R and python).


<br>
<br>

# Contrubutors :
<a href="https://github.com/Hamid-abdellaoui/Bitcoin-penetration/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Hamid-abdellaoui/Bitcoin-penetration" />
</a>
