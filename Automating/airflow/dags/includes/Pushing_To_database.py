import psycopg2
import pandas as pd


def creating_tables():
    try:
        conn = psycopg2.connect(database = "optionsdata", 
                                user = "openoptions", 
                                password = "openoptions", 
                                host = "postgres", 
                                port = "5432")
    except:
        print("Cannot connect to the database") 

    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS articles(titre VARCHAR ,text TEXT, date VARCHAR, link VARCHAR, img VARCHAR) ;")
    except:
        print("Cannot creat articles")
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS Economy_articles (bigrames CHARACTER(200), TF FLOAT,TFIQIDF FLOAT, mois CHARACTER(25));")
    except:
        print("Cannot creat Economy_articles")

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

        
        
        postgres_insert_query = """ INSERT INTO articles (titre ,text , date , link , img ) VALUES (%s,%s,%s,%s,%s)"""
        for i in range (len(df['date'])):
            record_to_insert = (df.titre[i], df.text[i], df.date[i], df.link[i], df.img[i])
            cursor.execute(postgres_insert_query, record_to_insert)
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
            
import psycopg2
import numpy as np
import psycopg2.extras as extras
import pandas as pd
  
  
def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
  
  
conn = psycopg2.connect(
    database="ENVIRONMENT_DATABASE", user='postgres', password='pass', host='127.0.0.1', port='5432'
)
  
df = pd.read_csv('fossilfuels.csv')
  
execute_values(conn, df, 'fossil_fuels_c02')