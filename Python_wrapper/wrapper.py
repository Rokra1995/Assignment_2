import pandas.io.sql as sqlio
import pandas as pd
import numpy as np
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





    #(6) generate any additional aggregate tables that are needed to answer the questions in the business case description.

    #miniumum requirements:


    # 1. Average  asking  price  per  month  for  each  of  the municipalities in the Netherlands
    #select needed columns from database and store them in a pandas dataframe
    executing_script = "SELECT sellingPrice, publicationDate, MunicipalityCode, MunicipalityName FROM funda_2018 NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names;"
    avg_asking_price = sqlio.read_sql_query(executing_script, conn)

    #create column month and year and create columns that will be used to group by
    avg_asking_price['month'] = avg_asking_price['publicationdate'].apply(lambda x: x.month)
    avg_asking_price['year'] = avg_asking_price['publicationdate'].apply(lambda x: x.year)
    groups = ['municipalityname', 'year','month']
    
    #group by selected columns. calcualte mean and safe as pandas Dataframe
    avg_asking_price_mean = avg_asking_price.groupby(by=groups).mean().reset_index()


    #2. Average asking price per bevolkingsdichtheid group or category (you might  have  to  discretize  this  variable)  for  each  gemeente  in  the  Netherlands
    #select needed columns from database and store them in a pandas dataframe
    executing_script = "SELECT DISTINCT sellingprice, districtCode, populationDensity, municipalityCode, municipalityname, zipcode FROM funda_2018 NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN district_info NATURAL LEFT JOIN municipality_names;"
    avg_asking_price_popdens = sqlio.read_sql_query(executing_script, conn)

    #avg_price_popdens_group_municipality
    #Cols: municipalitycode, popDens_category, avg-price

    #define function to discretize the variable populationDensity
    def discretizing(number):
        number = str(number)
        if number == 'None':
            number = 0
        elif number =='       .':
            number = 0
        number = int(number)
        if number <= 500:
            group = '<=500'
        elif (number > 500) & (number <= 1000):
            group = "501 - 1000"
        elif (number > 1000) & (number <= 1500):
            group = "1001 - 1500"
        elif (number > 1500) & (number <= 2000):
            group = "1501 - 2000"
        elif (number > 2000) & (number <= 3000):
            group = "2001 - 3000"
        elif (number > 3000) & (number <= 5000):
            group = "3001 - 5000"
        elif (number > 5000) & (number <= 7500):
            group = "5001 - 7500"
        elif (number > 7500) & (number <= 10000):
            group = "7501 - 10000"
        elif (number > 10000) & (number <= 15000):
            group = "10001 - 15000"
        elif (number > 15000):
            group = ">1500"
        return group

    #apple discretizing on the variable populationDensity and group by the density categorys and municiaplity and calculate mean
    avg_asking_price_popdens['population_dens_cat'] = avg_asking_price_popdens['populationdensity'].apply(lambda x: discretizing(x))
    avg_asking_price_popdens_grouped = avg_asking_price_popdens.groupby(by=['municipalitycode', 'population_dens_cat']).mean().reset_index()
    

    #3. Average  asking  price  per  gemeente,  where  the  gemeenten  are  ordered  according  to  the  average  income  per  inhabitant  (from  highest income to lowest income)
    executing_script = "SELECT sellingPrice, MunicipalityName, averageincomepercitizen FROM funda_2018 NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN municipality_info limit 1000;"
    avg_asking_price_by_income = sqlio.read_sql_query(executing_script, conn)
    avg_asking_price_by_income_sorted = avg_asking_price_by_income.sort_values('averageincomepercitizen',ascending=False)
    
    #avg_price_municipality_avg_citizien_income
    #Cols: municipalitycode, municaplityname, avg_price, avg_citizen_income
    
    #??: for  every  gemeente  in  the  Netherlands  and  every  month  in  2018-2019: the percentage increase or decrease in the average house price in that gemeente compared to the previous month
    #use the already grouped dataframe

    #avg_price_municipality_month_difference
    #Cols: municipalitycode, municipalityname, year, month, avg_price, percentage_diff

    print(avg_asking_price_mean)

    def calc_difference(index):
        print(index)
        rel_difference = 0
        return rel_difference
    
    test = avg_asking_price_mean.apply(lambda x: calc_difference(x), axis= 1)


    #Felicia: For  every  gemeente  in  the  Netherlands  and  every  month  in  2018-2019:  the  absolute  difference  between  the  median  house  price  for  that month in that gemeente and the median house price for the next month in that gemeente

    #Baris: The  average  house  price  in  2018-2019  according  to  leftijdgroep  (in  the whole of the Netherlands)

    #Emmanuel: Average sellingtime per month and municipality

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



