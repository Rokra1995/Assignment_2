# Felicia: Average  asking  price  per  gemeente,  where  the  gemeenten  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income)

# variables needed:
## averge selling price
## municipalitie names
## averega income per inhibitant
## from highest to lowest
# needed tables: funda_2018 (sellingPrice), zipcodes, municipality_names, municipality_info (AverageIncomePerCitizen)

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

    #Average  selling  price  per  municipality,  where  the  municipalities  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income)
    # Question: there was something with housing info right?
    avg_sellingprice_per_municipality__per_citizen_ordered_table = "SELECT sellingPrice, MunicipalityName, AverageIncomePerCitizen FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_info NATURAL LEFT JOIN housing_info;"
    avg_sellingprice_per_municipality__per_citizen = sqlio.read_sql_query(avg_sellingprice_per_municipality__per_citizen_ordered_table, conn)
    print(avg_sellingprice_per_municipality__per_citizen.sort_values('AverageIncomePerCitizen'))
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Analysis succesfully done')
funda_analysis()