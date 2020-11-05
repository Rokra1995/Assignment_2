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

# data cleaning

change_to_null = "UPDATE housing_data \
SET koopPrijs = NULL \
WHERE koopPrijs = 'NULL';"
#cur.execute(change_to_null)


#cur.execute("SELECT koopPrijs FROM housing_data WHERE koopPrijs IS NULL limit 50;")
#cur.execute("ALTER TABLE housing_data ADD COLUMN dateDiff integer GENERATED ALWAYS AS (datum_ondertekening - publicationDate) STORED")
cur.execute("Select array_length(regexp_split_to_array(volledigeOmschrijving, '\\s+'),1), volledigeOmschrijving from housing_data limit 1")
#cur.execute("SELECT (datum_ondertekening - publicationDate) from housing_data limit 3;")
#cur.execute("SELECT DATE_PART('day', '2012-01-01'::date) - DATE_PART('day', '2011-10-05'::date);")
#cur.execute("SELECT * FROM housing_data limit 1;")
#print(cur.fetchall())

#print(type(cur.fetchall()))
#cur.execute("SELECT datum_ondertekening, publicationDate, dateDiff FROM housing_data limit 3;")
print(cur.fetchall())

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()
