# Create the text search function - Felicia

import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector
import re

def text_search():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()
    
    #Create dataframe to select columns of housing_data
    word_in_description_table = "SELECT sellingPrice, fullDescription, houseType, categoryObject, yearOfBuilding, garden, parcelSurface, numberRooms, numberBathrooms, energylabelClass, surface FROM funda LIMIT 100;"
    word_in_description = sqlio.read_sql_query(word_in_description_table, conn)
    
    input_limit = 3
    counter = 0
    while counter < input_limit:
        word = input("Describe in one word what your house should look like: ")
        if not re.match("^[a-zA-Z\s]*$", word):
            print("Error! Only letters a-z allowed!")
            counter += 1
        elif " " in word:
            print("Error! Only 1 word allowed!")
            counter += 1
        else:
            print(word_in_description[word_in_description.fulldescription.str.contains(word)])
            counter = input_limit
text_search()  