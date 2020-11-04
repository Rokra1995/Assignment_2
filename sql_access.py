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

# Execute a command: this creates a new table
#cur.execute("DROP TABLE housing_data;")
#cur.execute("CREATE TABLE housing_data (globalId integer PRIMARY KEY, publicatieDatum text, postcode text, koopPrijs text, volledigeOmschrijving text, soortWoning text, categorieObject text, bouwjaar text, indTuin text, perceelOppervlakte text, kantoor_naam_MD5hash text, aantalKamers text, aantalBadkamers text, energielabelKlasse text, globalId1 integer, oppervlakte text, datum_ondertekening text);")
#cur.execute("COPY housing_data(globalId, publicatieDatum, postcode, koopPrijs, volledigeOmschrijving, soortWoning, categorieObject, bouwjaar, indTuin, perceelOppervlakte, kantoor_naam_MD5hash, aantalKamers, aantalBadkamers, energielabelKlasse, globalId1, oppervlakte, datum_ondertekening) FROM '/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/housing_data.csv' CSV HEADER;")

# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
#cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))

# Query the database and obtain data as Python objects
cur.execute("SELECT * FROM housing_data;")
print(cur.fetchone())

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()
 
 #Comment from Emmanuel ()test