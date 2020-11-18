# Correlations sellingprice/time and housing info - Felicia
# This needs to be added in the wrapper.py file!

import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

def correlation_housing_data_sellingprice_sellingtime():
    #start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()
    
    #Create dataframe to select columns of housing_data
    housinginfo_sellingpricetime_table = "SELECT sellingPrice, fullDescription, houseType, categoryObject, yearOfBuilding, garden, parcelSurface, numberRooms, numberBathrooms, energylabelClass, surface, sellingtime FROM funda;"
    housinginfo_sellingpricetime = sqlio.read_sql_query(housinginfo_sellingpricetime_table, conn)
    
    #Look for correlations between columns housing_data and sellingprice and sellingtime
    print(housinginfo_sellingpricetime.corr(method ='pearson')) 
    
    '''' 
    Conclusions with regard to sellingprice: 1)garden+sellingprice=-0,258484 2)parcelSurface+sellingprice=0.076516 3)numberrooms+sellingprice=0.100043 
    4)numberbathrooms+sellingprice=0.069725 5)surface+sellingprice=0.580748 6)sellingtime+sellingprice=0.145279
    
    Conclusion with reagrd to sellingtime: 1)garden+sellingtime=0.145279 2)garden+sellingtime=-0.085790 3)parcelsurface+sellingtime=0.002927 
    4)numberrooms+sellingtime= 0.136939 5)numberbathrooms+sellingtime=-0.073602 6)surface+sellingtime=0.153849
    '''
    
    return print('Analysis succesfully done')
correlation_housing_data_sellingprice_sellingtime()