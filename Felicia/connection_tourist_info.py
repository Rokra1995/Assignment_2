#ID = int64
#SoortRijksmonument = object
#RegioS = object
#Perioden = datetime64
#Rijksmonumenten_1 = int64

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

    tourist_info = pd.read_csv('/home/felies/Assignment_2/Felicia/municipality code and National monuments 2018.csv', sep=';') 

# inspect table structure & data types 
# tourist_info.info()

# cleaning of the data - rename columns
    tourist_info.rename(columns={'SoortRijksmonument': 'Type_of_national_monument', 'RegioS': 'Municipalitycode', 'Rijksmonumenten_1': 'Number_of_national_monuments'})
#print(tourist_info)

    from sqlalchemy import create_engine
#engine = create_engine("postgresql://felies:mupske@localhost:5433/funda")
#tourist_info.to_sql('tourist_info', engine)

    monuments_sellingtime_table = "SELECT avg_askingprice, avg_sellingtime, municipalityname, Number_of_national_monuments FROM aggregated_municipality_info NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN tourist_info"
    monuments_sellingtime = sqlio.read_sql_query(monuments_sellingtime_table, conn)
    print(monuments_sellingtime.head(10))
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Funda Data succesfully added')
funda_analysis()