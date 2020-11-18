# Story Create & clean python function to add tourist data - Felicia

import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

def add_tourist_info_to_database():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    tourist_info = pd.read_csv('/home/felies/Assignment_2/Felicia/municipality code and National monuments 2018.csv', sep=';') 

    #Cleaning of the data - rename columns
    tourist_info = tourist_info.rename(columns={'SoortRijksmonument': 'Type_of_national_monument', 'RegioS': 'Municipalitycode', 'Rijksmonumenten_1': 'Number_of_national_monuments'})

    #Specifiy tables to be created with their name and create them with the correct datatypes for postgres.
    db_tables = {'tourist_info':tourist_info}
    postgresql_dtype_translation = {'object':'text','int64':'integer','float64':'numeric','datetime64[ns]':'date'}
    
    for k,v in db_tables.items():
        command = "DROP TABLE IF EXISTS {} CASCADE;".format(k)
        print(command)
        cur.execute(command)
        conn.commit()
        cols = ",".join(["{} {}".format(key, postgresql_dtype_translation.get(str(val))) for key,val in v.dtypes.items()])
        command = "CREATE TABLE IF NOT EXISTS {} ({});".format(k, cols)
        cur.execute(command)
        print(command)
        conn.commit()
    
    #Fill tables one by one with info:
    for k,v in db_tables.items():
        cols = ",".join([str(i) for i in v.columns.tolist()])
        for idx,row in v.iterrows():
            row = list(row)
            for idx, element in enumerate(row):
                cleaned = element if type(element) != str else element.replace("'","")
                row[idx] = cleaned
            sql = "INSERT INTO {} ({}) VALUES {}".format(str(k),cols,tuple(row))
            cur.execute(sql)
            conn.commit()
        print("Table {} succesfully filled with data".format(str(k)))
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Tourist info succesfully added')
add_tourist_info_to_database()

def tourist_info_analysis ():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()
    
    #Select municipality name, sellingprice, sellingtime and number of national monuments
    tourist_info_sellingtime_and_price_table = "SELECT sellingPrice, MunicipalityName, sellingtime, number_of_national_monuments FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN tourist_info;"
    tourist_info_sellingtime_and_price = sqlio.read_sql_query(tourist_info_sellingtime_and_price_table, conn)
    #print(tourist_info_sellingtime_and_price.groupby(['municipalityname']).head(10))
    
    #Look for correlations between number of monuments (tourist info) and sellingprice and sellingtime
    print(tourist_info_sellingtime_and_price.corr(method ='pearson',min_periods=3)) 
    
    '''
    Conclusions: 
    1) correlation number_of_national_monuments+sellingprice = 0.271252 (slightly positivie) 
    2) correlation number_of_national_monuments+sellingtime = -0.155473 (slightly negative) 
    '''
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Tourist info analysis succesfully done')
tourist_info_analysis()