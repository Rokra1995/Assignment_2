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
    #Example:
    # Select the first 100 rows in the funda table and fetch them to a list object
    executing_script = "SELECT sellingPrice, publicationDate, MunicipalityCode, MunicipalityName FROM funda_2018 NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names;"
    funda = sqlio.read_sql_query(executing_script, conn)
    print(funda)



    #(6) generate any additional aggregate tables that are needed to answer the questions in the business case description.

    #miniumum requirements:

    #Average  asking  price  per  month  for  each  of  the municipalities in the Netherlands
    
    #Table sellingPrice, publicationDate, municipalityCode
    print(funda.publicationdate.iloc[0].month)
    funda['month'] = funda['publicationdate'].apply(lambda x: x.month)
    funda['year'] = funda['publicationdate'].apply(lambda x: x.year)

    groups = ['municipalityname', 'year','month']
    salesprice_mean = funda.groupby(by=groups).mean().reset_index()
    print(salesprice_mean)


    #Robin: Average asking price per bevolkingsdichtheid group or category (you might  have  to  discretize  this  variable)  for  each  gemeente  in  the  Netherlands

    #Felicia: Average  asking  price  per  gemeente,  where  the  gemeenten  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income)

    #??: for  every  gemeente  in  the  Netherlands  and  every  month  in  2018-2019: the percentage increase or decrease in the average house price in that gemeente compared to the previous month

    #???: For  every  gemeente  in  the  Netherlands  and  every  month  in  2018-2019:  the  absolute  difference  between  the  median  house  price  for  that month in that gemeente and the median house price for the next month in that gemeente

    #Baris: The  average  house  price  in  2018-2019  according  to  leftijdgroep  (in  the whole of the Netherlands)

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



