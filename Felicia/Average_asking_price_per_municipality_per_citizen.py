# Felicia: Average  asking  price  per  gemeente,  where  the  gemeenten  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income)

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
    #Felicia: Average  selling  price  per  municipality,  where  the  municipalities  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income)
    executing_script_Q2 = "SELECT sellingPrice, MunicipalityName, AverageIncomePerCitizen FROM funda_2018 NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN municipality_info;"
    funda_Q2 = sqlio.read_sql_query(executing_script_Q2, conn)
    print(funda_Q2.sort_values('AverageIncomePerCitizen'))
    
    #rank column AverageIncomePerCitizen
    
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Analysis succesfully done')
funda_analysis()
    
#Felicia: Average  selling  price  per  municipality,  where  the  municipalities  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income)
# needed tables: funda_2018 (sellingPrice), zipcodes, municipality_names, municipality_info (AverageIncomePerCitizen)
# I need to join funda_2017 to zipcodes, zipcodes to municipality_names and municipality_names to municipality_info
