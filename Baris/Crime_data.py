# Story Create & clean python function to add tourist data - Baris

import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

def add_crime_info_to_database():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    crime_info = pd.read_csv(R"Registeredcrimesandmunicipalitycode.csv", sep=';') 

    #Clean all the nan within the dataframe
    crime_info = crime_info.fillna('-')

    #Cleaning of the data - rename columns
    crime_info = crime_info.rename(columns={'SoortMisdrijf': 'Sort_of_crime', 'RegioS': 'Municipalitycode', 'Perioden': 'Periods', 'TotaalGeregistreerdeMisdrijven_1': 'Number_of_registered_crimes', 'GeregistreerdeMisdrijvenRelatief_2': 'Relatively_registered_crimes', 'GeregistreerdeMisdrijvenPer1000Inw_3': 'Registered_crimes_per_1000_inhabitants', 'TotaalOpgehelderdeMisdrijven_4': 'Total_clarified_crimes', 'OpgehelderdeMisdrijvenRelatief_5': 'Relatively_clarified_crimes', 'RegistratiesVanVerdachten_6': 'Registrations_of_suspects'})

    #Specifiy tables to be created with their name and create them with the correct datatypes for postgres.
    db_tables = {'crime_info':crime_info}
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
    return print('Crime info succesfully added')
add_crime_info_to_database()

def crime_info_analysis ():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()
    
    sellingtime_and_price_table = "SELECT sellingPrice, MunicipalityCode, MunicipalityName, sellingtime FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names limit 100;"
    sellingtime_and_price = sqlio.read_sql_query(sellingtime_and_price_table, conn)

    #Select municipality name, sellingprice and sellingtime 
    #print(sellingtime_and_price.groupby(['municipalityname']).reset_index())
    
    #Select municipality name, sellingprice, sellingtime and number of national monuments
    crime_info_sellingtime_and_price_table = "SELECT sellingPrice, MunicipalityName, sellingtime, Number_of_registered_crimes FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN crime_info;"
    crime_info_sellingtime_and_price = sqlio.read_sql_query(crime_info_sellingtime_and_price_table, conn)
    #print(tourist_info_sellingtime_and_price.groupby(['municipalityname']).head(10))
    
    #Look for correlations between number of monuments (tourist info) and sellingprice and sellingtime
    print(crime_info_sellingtime_and_price.corr(method ='pearson',min_periods=3)) 
    
    '''
    Conclusions: 
    1) correlation number_of_registered_crimes+sellingprice = 1.0 (Perfect correlation) 
    2) correlation number_of_registered_crimes+sellingtime = 0.156633 (slightly positive) 
    '''
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Crime info analysis succesfully done')
crime_info_analysis()