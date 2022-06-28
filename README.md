# BI-text-mining

An academic project that aims to extract and process text from a large amount of articles scrapped from many Moroccan news Websites.

This project is divided into 5 parts, each part is in an independent directory:




- **[Part 1]**: **[Scraping](/Scraping/)**
- **[Part 2]**: **[Text_processing](/Text_processing/)**
- **[Part 3]**: **[Automating](/Automating/)**
- **[Part 4]**: **[Reporting](/Reporting/)**
- **[Part 5]**: **[Mining](/Mining/)**

<br>

# Further Details

## 1. **[Scraping](/Scraping/)** : 
Scrapping articles (Title, publiction date, Image, Link, Full text...)from Moroccan news websites(BeatifulSoup and requests).

### <b> Ressources : </b>

Data was retrieved from the following websites:
> [Le matin.ma](https://www.lematin.ma/) <br>
> [La vie eco](https://www.lavieeco.com) <br>
> [Challenge.ma](https://www.challenge.ma/) <br>

### <b> Data structure : </b>

> We scrapped the Economy subcatoey pages for each news website. for each article we got its :
- `title` 
- `publication date`
- `image`
- `link`
- `full text`.

## 2. **[Text_processing](/Text_processing/)** : 
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

## 3. **[Automating](/Automating/)** : 
Automate the process of scraping, text processing, Datawarehousing and loading Data into Postgresql Database(Airflow, Docker...).
The Datapipeline architecture is as follows:

<!-- image -->
![](/Automating/data/Image1.png)

## 4. **[Reporting](/Reporting/)** : 
    Present results and key measures in a dashboard (Web app with Flask).

Reporting results via a simple dashboard as follows:
<!-- IMAGE -->
![](/Reporting/Dahboarding%20App/static/images/Image1.png)
## 5. **[Mining](/Mining/)** : 
Extract association rules (R and python).


<br>
<br>

# Contrubutors :
<a href="https://github.com/Hamid-abdellaoui/Bitcoin-penetration/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Hamid-abdellaoui/Bitcoin-penetration" />
</a>
