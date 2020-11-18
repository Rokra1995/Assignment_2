# SQL query 3 - Felicia
# Average  asking  price  per  municipality are  ordered  to  the  average  income  per  inhabitant  (from  highest income to lowest income)
# tables: funda (sellingPrice), zipcodes, municipality_names, municipality_info (averageincomepercitizen)
# variables: average selling price, municipalitie names, average income per inhibitant

import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

def avg_sellingprice_per_municipality_ranked_by_avg_income_calculation():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    #Select needed columns from database and store them in a pandas dataframe
    sellingprice_per_municipality__per_citizen_ordered_table = "SELECT sellingPrice, MunicipalityName, MunicipalityCode, averageincomepercitizen FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN (SELECT MunicipalityCode, averageincomepercitizen from housing_info) AS housing_info;"
    sellingprice_per_municipality__per_citizen = sqlio.read_sql_query(sellingprice_per_municipality__per_citizen_ordered_table, conn)
    
    #Select the avg asking price per municipality ranked by avg income 
    print(sellingprice_per_municipality__per_citizen.groupby(['municipalityname']).mean().sort_values('averageincomepercitizen', ascending=False).head(50))  
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Average sellingprice per municipality ranked by income succesfully calculated')
avg_sellingprice_per_municipality_ranked_by_avg_income_calculation()