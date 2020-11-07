from sqlalchemy import create_engine
import pandas.io.sql as sqlio
import pandas as pd
import psycopg2
import mysql.connector

#DB Log in
#DB Name: RobinKratschmayr2
#Username: RobinKratschmayr2
#Host: localhost
#Port: 5432

engine = create_engine('postgresql://localhost/[Robinkratschmayr2]')

#Connect to an existing database
#conn = psycopg2.connect("dbname=Robinkratschmayr2 user=Robinkratschmayr2")

#Connection on the RPI
#change it to your credentials
conn = psycopg2.connect("host=localhost user=pi password=raspberry dbname=test")

# Open a cursor to perform database operations
cur = conn.cursor()

# Select the first 100 rows in the funda table and fetch them to a list object
executing_script = "SELECT * FROM funda_2018 limit 100;"
funda_2018 = sqlio.read_sql_query(executing_script, conn)
print(funda_2018)

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()


