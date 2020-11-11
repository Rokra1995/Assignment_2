import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

'''
Good morning, I am getting some questions from you about what the python wrapper should be. 
What I am looking for is a python script that can be used to (1) load the funda data from csv, 
(2) preprocess the data, (3) connect to the database, 
(4) upload the data into a database table for year 2018, 
(5) run a query against this table to enrich it with the CBS data, 
(6) generate any additional aggregate tables that are needed to answer the questions in the business case description. 

The idea is that the python wrapper should do all of this automatically. 
'''


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
    #Example:
    # Select the first 100 rows in the funda table and fetch them to a list object
    executing_script = "SELECT * FROM funda_2018 limit 100;"
    funda = sqlio.read_sql_query(executing_script, conn)
    print(funda)

    #(6) generate any additional aggregate tables that are needed to answer the questions in the business case description.

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

def full_text_search(text):

    '''
    SELECT title
    FROM pgweb
    WHERE to_tsvector('english',body) @@ to_tsquery('friend');
    '''

    return DF



