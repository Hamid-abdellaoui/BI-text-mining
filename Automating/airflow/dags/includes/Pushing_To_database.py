import psycopg2
import pandas as pd

#----------------------------------------------------------------#
#-            doing some data transformations                   -#
#----------------------------------------------------------------#


# dataset
dataset = pd.read_csv('data/Dataset/data.csv', parse_dates=['date'])
dataset['period']=dataset['date'].dt.to_period('M')
dataset.sort_values(by="date" ,inplace=True)
one_month = list(set(dataset["period"]))[1]
dataset = dataset[dataset["period"]== one_month].transpose()
dataset.columns=[ i for i in range(1,len(dataset.columns)+1)]
df = dataset.transpose()

# topics indexing
topics = pd.read_csv('data/Outputs/topics.csv', index_col='doc_num')
topics_columns = []
i=0
for topic in topics.columns:
    topics_columns.append(['topic_' + str(i) , topic])
    i=i+1
topics_columns=pd.DataFrame(topics_columns,columns=['index','topic'])
topics.columns = topics_columns['index']

# appriori indexing
# appriori = pd.read_csv('data/Outputs/appriori.csv')
# appriori_columns = []
# i=0
# for topic in appriori.columns:
#     appriori_columns.append(['bigramm_' + str(i) , topic])
#     i = i + 1
# appriori_columns = pd.DataFrame(appriori_columns,columns=['index','topic'])
# appriori.columns = appriori_columns['index']

# indexing trends
trends = pd.read_csv('data/Outputs/trends.csv')
trends.fillna(0, inplace=True)
trends_columns = []
i=0
for topic in trends.columns:
    trends_columns.append(['trend_' + str(i) , topic])
    i = i + 1
trends_columns = pd.DataFrame(trends_columns,columns=['index','topic'])
trends.columns = trends_columns['index']




#----------------------------------------------------------------#
#-            pushing data to database                          -#
#----------------------------------------------------------------#

def creating_table1():
    # connecting to DB
    try:
        conn = psycopg2.connect(database = "optionsdata", 
                                user = "openoptions", 
                                password = "openoptions", 
                                host = "postgres", 
                                port = "5432")
    except:
        print("Cannot connect to the database") 

    cur = conn.cursor()
    
    # 1- creating table articles 
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS articles(art_id INTEGER PRIMARY KEY, titre VARCHAR ,text TEXT, date VARCHAR, link VARCHAR, img VARCHAR, period VARCHAR) ;""")
    except:
        print("Cannot creat table")
    
    conn.commit()
    conn.close()
    cur.close()
    
def creating_table2():
    # connecting to DB
    try:
        conn = psycopg2.connect(database = "optionsdata", 
                                user = "openoptions", 
                                password = "openoptions", 
                                host = "postgres", 
                                port = "5432")
    except:
        print("Cannot connect to the database") 

    cur = conn.cursor()
    
    # 2- creating table topics
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS topics(topic_0 float, topic_1 float, topic_2 float, topic_3 float, topic_4 float, topic_5 float, topic_6 float, topic_7 float, topic_8 float, topic_9 float  ;""")
    except:
        print("Cannot creat topics")
    
    conn.commit()
    conn.close()
    cur.close()

def creating_table3():
    # connecting to DB
    try:
        conn = psycopg2.connect(database = "optionsdata", 
                                user = "openoptions", 
                                password = "openoptions", 
                                host = "postgres", 
                                port = "5432")
    except:
        print("Cannot connect to the database") 

    cur = conn.cursor()
    
    # 3- creating table trends
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS trends(trend_0 float, trend_1 float, trend_2 float, trend_3 float, trend_4 float ;""")
    except:
        print("Cannot creat trends")
    
    conn.commit()
    conn.close()
    cur.close()

def creating_table4():
    # connecting to DB
    try:
        conn = psycopg2.connect(database = "optionsdata", 
                                user = "openoptions", 
                                password = "openoptions", 
                                host = "postgres", 
                                port = "5432")
    except:
        print("Cannot connect to the database") 

    cur = conn.cursor()
    
        # 4- creating table topics_columns
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS topics_columns(index VARCHAR, topic VARCHAR) ;""")
    except:
        print("Cannot creat topics_columns")
    
    conn.commit()
    conn.close()
    cur.close()  

def creating_table5():
    # connecting to DB
    try:
        conn = psycopg2.connect(database = "optionsdata", 
                                user = "openoptions", 
                                password = "openoptions", 
                                host = "postgres", 
                                port = "5432")
    except:
        print("Cannot connect to the database") 

    cur = conn.cursor()
    
        # 5- creating table trends_columns
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS trends_columns(index VARCHAR, topic VARCHAR) ;""")
    except:
        print("Cannot creat trends_columns")
    
    conn.commit()
    conn.close()
    cur.close()





def loading_to_database():
    try:
        connection = psycopg2.connect(user="openoptions",
                                    password="openoptions",
                                    host="postgres",
                                    port="5432",
                                    database="optionsdata")
        cursor = connection.cursor()

        
        # inserting df to articles
        postgres_insert_query = """ INSERT INTO articles (id, titre ,text , date , link , img, period ) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        for i in range (len(df['date'])):
            record_to_insert = (df.index[i], df.titre[i], df.text[i], df.date[i], df.link[i], df.img[i], df.period[i])
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")
        
        # inserting topics to topics
        query_2 = """ INSERT INTO topics (topic_0, topic_1, topic_2, topic_3, topic_4, topic_5, topic_6, topic_7, topic_8, topic_9) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        for i in range (len(topics)):
            record_to_insert = (topics.iloc[i])
            cursor.execute(query_2, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")
            
        # inserting trends to trends
        query_3 = """ INSERT INTO trends (trend_0, trend_1, trend_2, trend_3, trend_4) VALUES (%s,%s,%s,%s,%s)"""
        for i in range (len(trends)):
            record_to_insert = (trends.iloc[i])
            cursor.execute(query_3, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")
            
        # inserting topics_columns to topics_columns
        query_4 = """ INSERT INTO topics_columns (index, topic) VALUES (%s,%s)"""
        for i in range (len(topics_columns)):
            record_to_insert = (topics_columns.iloc[i])
            cursor.execute(query_4, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
