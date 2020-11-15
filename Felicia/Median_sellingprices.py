# Felicia: the  absolute  difference  between  the  median  house  price  for  that month in that municipality and the median house price for the next month in that municipality

# variables needed:
## averge selling price
## municipalitie names
## averega income per inhibitant
## from highest to lowest

import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

def add_funda_data(csv_path, year):
    #start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    ######WRITE THE CODE HERE############################
    '''
    (1) load the funda data from csv, 
    (2) preprocess the data, 
    (3) connect to the database, 
    (4) upload the data into a database table for year 2018,
    '''

    ######################################################

    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Funda Data succesfully added')

def funda_analysis():
    #start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    ######WRITE THE CODE HERE############################

#Felicia: For  every  municipality and  every  month  in  2018:  
# the  absolute  difference  between  the  median  house  price  for  that month in that municipality and the median house price for the next month in that municipality
# needed tables: funda_2018 sellingPrice, zipcodes and municipality_names (municipality_Name) 
# needed values: median of sellingPrice and monthly division 

    executing_script_median_prices = "SELECT sellingPrice, MunicipalityName FROM funda_2018 NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN"
    monthly_median['grade'] = sqlio.read_sql_query(executing_script_median_prices, conn)
    monthly_median['month'] = monthly_median['sellingPrice'].apply(lambda x: x.month)
    print(monthly_median)

    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Funda Data succesfully added')
funda_analysis()