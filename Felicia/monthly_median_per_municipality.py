import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

def funda_analysis():
    #start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    # Felicia: For  every  municipality and  every  month  in  2018:  
    # the  absolute  difference  between  the  median  house  price  for  that month in that municipality and the median house price for the next month in that municipality
    # tables: funda sellingPrice, zipcodes and municipality_names 
    # needed values: median of sellingPrice per municipalityname and monthly division 

    monthly_median_per_municipality_table = "SELECT sellingPrice, publicationDate, MunicipalityName FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names"
    monthly_median_per_municipality = sqlio.read_sql_query(monthly_median_per_municipality_table, conn)
    
    # group by municipality and month
    # set publicationdate to datetime_format
    monthly_median_per_municipality['publicationdate'] = pd.to_datetime(monthly_median_per_municipality['publicationdate'])
    
    # calculate difference between the prices
    # monthly median per municipality
    # print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median()) 
    # difference between #monthly median per municipality
    # print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median().diff()) 
    
    # calculate absolute difference between the prices
    print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median().diff().abs())
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Funda Data succesfully added')
funda_analysis()

