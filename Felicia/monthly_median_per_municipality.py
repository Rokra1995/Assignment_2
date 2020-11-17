# SQL query 5 - Felicia
# For  every  municipality and  every  month  in  2018:  
# the  absolute  difference  between  the  median  house  price  for  that month in that municipality and the median house price for the next month in that municipality
# tables: funda sellingPrice, zipcodes and municipality_names 
# variables: median of sellingPrice per municipalityname and monthly division 

import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

def monthly_median_per_municipality_calculation():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    monthly_median_per_municipality_table = "SELECT sellingPrice, publicationDate, MunicipalityName FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names"
    monthly_median_per_municipality = sqlio.read_sql_query(monthly_median_per_municipality_table, conn)
    
    #Group by municipality and month
    #Set publicationdate to datetime_format
    monthly_median_per_municipality['publicationdate'] = pd.to_datetime(monthly_median_per_municipality['publicationdate'])
    
    #Calculate difference between the prices
    #Monthly median per municipality
    #print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median()) 
    #Difference between monthly median per municipality
    #print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median().diff()) 
    
    #Calculate absolute difference between the prices
    print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median().diff().abs())
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Monthly median sellingprice per municipality succesfully calculated')
monthly_median_per_municipality_calculation()

