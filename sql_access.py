from sqlalchemy import create_engine
import pandas as pd
import psycopg2

#DB Log in
#DB Name: RobinKratschmayr2
#Username: RobinKratschmayr2
#Host: localhost
#Port: 5432

#data = pd.read_csv('housing_data.csv')
#print(data.head())
#print(data.columns)
#print(data.iloc[0])

engine = create_engine('postgresql://localhost/[Robinkratschmayr2]')

#Connect to an existing database
conn = psycopg2.connect("dbname=Robinkratschmayr2 user=Robinkratschmayr2")

# Open a cursor to perform database operations
cur = conn.cursor()

# data cleaning

executing_script = "SELECT * FROM funda_2018 limit 10;"
cur.execute(executing_script)
print(cur.fetchall())

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()
