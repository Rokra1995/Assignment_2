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
    #Example:
    # Select the first 100 rows in the funda table and fetch them to a list object
    executing_script = "SELECT * FROM funda_2018 limit 100;"
    funda_2018 = sqlio.read_sql_query(executing_script, conn)
    print(funda_2018)

    #miniumum requirements:

    #Average  asking  price  per  month  for  each  of  the  gemeenten  and  municipalities in the Netherlands

    #Average asking price per bevolkingsdichtheid group or category (you might  have  to  discretize  this  variable)  for  each  gemeente  in  the  Netherlands

    #Average  asking  price  per  gemeente,  where  the  gemeenten  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income

    #for  every  gemeente  in  the  Netherlands  and  every  month  in  2018-2019: the percentage increase or decrease in the average house price in that gemeente compared to the previous month

    #For  every  gemeente  in  the  Netherlands  and  every  month  in  2018-2019:  the  absolute  difference  between  the  median  house  price  for  that month in that gemeente and the median house price for the next month in that gemeente

    #The  average  house  price  in  2018-2019  according  to  leftijdgroep  (in  the whole of the Netherlands)

    ######################################################


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Analysis succesfully done')

funda_analysis()

