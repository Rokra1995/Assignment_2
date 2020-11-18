import pandas.io.sql as sqlio
import pandas as pd
import numpy as np
import psycopg2
import mysql.connector
import os
import sys
import datetime
import matplotlib.pyplot as plt

# a function to identify the Rootpath necessary to load the csv in the initialize database function
def splitPath(s):
    f = os.path.basename(s)
    p = s[:-(len(f))-1]
    return f, p

def category_to_list(item):
    item_list = item.split(' ')
    cleaned_item_list = []
    for i in item_list:
        cleaned_item = i.replace('<','').replace('{','').replace('>','').replace('}','').replace('(','').replace(')','')
        cleaned_item_list.append(cleaned_item)
    return str(cleaned_item_list)

def initialize_database():
    #start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    current_path = os.path.dirname(os.path.abspath(__file__))
    f,root = splitPath(current_path)

    #Read csv input files:
    funda = pd.read_csv(os.path.join(root, 'Input_data/housing_data.csv'))
    cbs_data = pd.read_csv(os.path.join(root, 'Input_data/cbs_data.csv'), sep=";")
    brt_2020 = pd.read_csv(os.path.join(root, 'Input_data/brt2020.csv'), sep=";")
    postcodes = pd.read_csv(os.path.join(root, 'Input_data/pc6-gwb2020.csv'), sep=";")


    #remove NoneTypes, rename columns to english and drop unnecessary columns
    #also cleaning the category objects to python list types for later usage and calculating the sellingtime
    funda_cleaned = funda.fillna(0).rename(columns={'publicatieDatum':'publicationDate','postcode':'zipcode','koopPrijs':'sellingPrice',\
    'volledigeOmschrijving':'fullDescription','soortWoning':'houseType','categorieObject':'categoryObject','bouwjaar':'yearOfBuilding', \
    'indTuin':'garden','perceelOppervlakte':'parcelSurface','aantalKamers':'numberRooms','aantalBadkamers':'numberBathrooms','energielabelKlasse':'energylabelClass',\
    'oppervlakte':'surface','datum_ondertekening':'sellingDate'}).drop(['globalId', 'globalId.1','kantoor_naam_MD5hash'], axis=1)

    funda_cleaned['houseType'] = funda_cleaned['houseType'].apply(lambda x: category_to_list(x))
    funda_cleaned['categoryObject'] = funda_cleaned['categoryObject'].apply(lambda x: str(x).replace('<','').replace('{','').replace('>','').replace('}',''))
    funda_cleaned['sellingTime'] = pd.to_datetime(funda_cleaned['sellingDate']) - pd.to_datetime(funda_cleaned['publicationDate'])
    funda_cleaned['sellingTime'] = funda_cleaned['sellingTime'].apply(lambda x: int(x.days))

    cbs_cleaned = cbs_data.fillna(0).rename(columns={'WijkenEnBuurten':'NeighborhoodsAndDistricts','Gemeentenaam_1':'NameOfMunicipality','Mannen_6':'NumberOfMen',\
    'Vrouwen_7':'NumberOfWomen','k_0Tot15Jaar_8':'AgeFrom0to15years','k_15Tot25Jaar_9':'AgeFrom15to25years',\
    'k_25Tot45Jaar_10' : 'AgeFrom25to45years','k_45Tot65Jaar_11' : 'AgeFrom45to65years','k_65JaarOfOuder_12' : 'AgeFrom65AndOlder',\
    'Bevolkingsdichtheid_33' : 'PopulationDensity','Woningvoorraad_34' : 'HousingStock','PercentageBewoond_38' : 'PercentageInhabited',\
    'PercentageOnbewoond_39' : 'PercentageUninhabited','Koopwoningen_40' : 'OwnerOccupiedHouses','HuurwoningenTotaal_41' : 'RentalHouses',\
    'BouwjaarVoor2000_45' : 'ConstructionYearBefore2000','BouwjaarVanaf2000_46' : 'ConstructionYearAfter2000',\
    'GemiddeldInkomenPerInwoner_66' : 'AverageIncomePerCitizen','MeestVoorkomendePostcode_103' : 'MostCommonPostalCode','Dekkingspercentage_104' : 'CoveragePercentage'}).drop(['Codering_3','NameOfMunicipality'], axis=1).replace(' ','').replace('       .',0)
    cbs_cleaned = cbs_cleaned.apply(lambda x: x.str.strip() if x.dtype == "object" else x).replace('.',0).fillna(0)
    type_dict = {'PopulationDensity': 'float64','PercentageInhabited':'float64','PercentageUninhabited': 'float64','OwnerOccupiedHouses': 'float64','RentalHouses': 'float64','ConstructionYearBefore2000': 'float64','ConstructionYearAfter2000': 'float64','AverageIncomePerCitizen': 'float64','CoveragePercentage': 'float64'}

    for k,v in type_dict.items():
        cbs_cleaned = cbs_cleaned.astype({k: v})
    
    cbs_cleaned['NeighborhoodsAndDistricts'] = cbs_cleaned['NeighborhoodsAndDistricts'].replace(' ','')
    cbs_cleaned['MunicipalityCode'] = cbs_cleaned['NeighborhoodsAndDistricts'].apply(lambda x: str(x) if str(x).startswith('GM') else '-')
    cbs_cleaned['DistrictCode'] = cbs_cleaned['NeighborhoodsAndDistricts'].apply(lambda x: str(x) if str(x).startswith('WK') else '-')
    cbs_cleaned['NeighborhoodCode'] = cbs_cleaned['NeighborhoodsAndDistricts'].apply(lambda x: str(x) if str(x).startswith('BU') else '-')
    

    demographic_info_db = cbs_cleaned[['MunicipalityCode','DistrictCode','NeighborhoodCode','NumberOfMen','NumberOfWomen','AgeFrom0to15years','AgeFrom15to25years',\
    'AgeFrom25to45years','AgeFrom45to65years','AgeFrom65AndOlder','PopulationDensity']]
    
    housing_info_db = cbs_cleaned[['MunicipalityCode','DistrictCode','NeighborhoodCode','HousingStock','PercentageInhabited',\
    'PercentageUninhabited','OwnerOccupiedHouses','RentalHouses','ConstructionYearBefore2000','ConstructionYearAfter2000',\
    'AverageIncomePerCitizen','MostCommonPostalCode','CoveragePercentage']]

    municipality_names_db = brt_2020[['GM_NAAM','GM_2020']].drop_duplicates().rename(columns={'GM_NAAM':'MunicipalityName','GM_2020':'MunicipalityCode'}).replace("'","").drop_duplicates(subset='MunicipalityCode', keep='first')
    district_names_db = brt_2020[['WK_NAAM','WK_2020']].drop_duplicates().rename(columns={'WK_NAAM':'DistrictName','WK_2020':'DistrictCode'}).drop_duplicates(subset='DistrictCode', keep='first')
    buurt_names_db = brt_2020[['buurtnaam2020','buurtcode2020']].drop_duplicates().rename(columns={'buurtnaam2020':'NeighborhoodName','buurtcode2020':'NeighborhoodCode'}).drop_duplicates(subset='NeighborhoodCode', keep='first').astype({'NeighborhoodCode':'object'})
    postcodes_db = postcodes.merge(brt_2020, left_on='Buurt2020',right_on='buurtcode2020', how='left')[['PC6','Buurt2020','GM_2020','WK_2020']].rename(columns={'PC6':'zipcode','Buurt2020':'NeighborhoodCode','GM_2020':'MunicipalityCode','WK_2020':'DistrictCode'}).astype({'NeighborhoodCode':'object'}).drop_duplicates(subset='zipcode', keep="first")

    #specifiy tables to be created with their name and create them with the correct datatypes for postgres.
    db_tables = {'funda':funda_cleaned,'demographic_info':demographic_info_db,'housing_info':housing_info_db,'municipality_names':municipality_names_db,'district_names':district_names_db,'Neighborhood_names':buurt_names_db,'zipcodes':postcodes_db}
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
    
    #fill tables one by one with info:
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

    #specifiy the Keys:
    key_commands = [
        "ALTER TABLE funda ADD COLUMN ID SERIAL PRIMARY KEY;",
        "ALTER TABLE zipcodes ADD PRIMARY KEY (zipcode)",
        "ALTER TABLE funda ADD FOREIGN KEY (zipcode) REFERENCES zipcodes(zipcode);",
        "ALTER TABLE municipality_names ADD PRIMARY KEY (MunicipalityCode);",
        "ALTER TABLE district_names ADD PRIMARY KEY (DistrictCode);",
        "ALTER TABLE neighborhood_names ADD PRIMARY KEY (NeighborhoodCode);",
        "ALTER TABLE housing_info ADD COLUMN ID SERIAL PRIMARY KEY;",
        "ALTER TABLE demographic_info ADD COLUMN ID SERIAL PRIMARY KEY;",
        "ALTER TABLE zipcodes ADD FOREIGN KEY (MunicipalityCode) REFERENCES municipality_names(MunicipalityCode);",
        "ALTER TABLE zipcodes ADD FOREIGN KEY (DistrictCode) REFERENCES district_names(DistrictCode);",
        "ALTER TABLE zipcodes ADD FOREIGN KEY (NeighborhoodCode) REFERENCES neighborhood_names(NeighborhoodCode);",
    ]

    for SQL in key_commands:
        try:
            print(SQL)
            cur.execute(SQL,conn)
            conn.commit()
        except Exception as E:
            print(E)
            conn.rollback()
    
    #End connection
    cur.close()
    conn.close()

    return print('Database successfully initialized')

def add_funda_data():
    csv_path = input("Enter the path to the funda csv file you want to add:")
    #start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    #(1) load the funda data from csv, 
    funda = pd.read_csv(os.path.join(str(csv_path)))

    #remove NoneTypes, rename columns to english and drop unnecessary columns
    #also cleaning the category objects to python list types for later usage and calculating the sellingtime
    funda_cleaned = funda.fillna(0).rename(columns={'publicatieDatum':'publicationDate','postcode':'zipcode','koopPrijs':'sellingPrice',\
    'volledigeOmschrijving':'fullDescription','soortWoning':'houseType','categorieObject':'categoryObject','bouwjaar':'yearOfBuilding', \
    'indTuin':'garden','perceelOppervlakte':'parcelSurface','aantalKamers':'numberRooms','aantalBadkamers':'numberBathrooms','energielabelKlasse':'energylabelClass',\
    'oppervlakte':'surface','datum_ondertekening':'sellingDate'}).drop(['globalId', 'globalId.1','kantoor_naam_MD5hash'], axis=1)

    funda_cleaned['houseType'] = funda_cleaned['houseType'].apply(lambda x: category_to_list(x))
    funda_cleaned['categoryObject'] = funda_cleaned['categoryObject'].apply(lambda x: str(x).replace('<','').replace('{','').replace('>','').replace('}',''))
    funda_cleaned['sellingTime'] = pd.to_datetime(funda_cleaned['sellingDate']) - pd.to_datetime(funda_cleaned['publicationDate'])
    funda_cleaned['sellingTime'] = funda_cleaned['sellingTime'].apply(lambda x: int(x.days))
    
    cols = ",".join([str(i) for i in funda_cleaned.columns.tolist()])
    for idx,row in funda_cleaned.iterrows():
        row = list(row)
        for idx, element in enumerate(row):
            cleaned = element if type(element) != str else element.replace("'","")
            row[idx] = cleaned
        sql = "INSERT INTO funda ({}) VALUES {}".format(cols,tuple(row))
        cur.execute(sql)
        conn.commit()
    print("Data succesfully added to the funda table")

    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Funda Data succesfully added')

def test():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    executing_script = "SELECT * FROM (SELECT municipalityCode, zipcode FROM funda NATURAL LEFT JOIN zipcodes as funda_zip limit 1000) AS foo NATURAL LEFT JOIN demographic_info;"
    avg_asking_price_popdens = sqlio.read_sql_query(executing_script, conn)
    print(avg_asking_price_popdens)


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return DF

#Query 1
def query_1():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    #executing_script = "SELECT * FROM (SELECT municipalityCode, zipcode FROM funda NATURAL LEFT JOIN zipcodes as funda_zip limit 1000) AS foo NATURAL LEFT JOIN demographic_info;"
    # 1. Average  asking  price  per  month  for  each  of  the municipalities in the Netherlands
    #select needed columns from database and store them in a pandas dataframe
    executing_script = "SELECT sellingPrice, publicationDate, MunicipalityCode, MunicipalityName FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names;"
    avg_asking_price = sqlio.read_sql_query(executing_script, conn)
    avg_asking_price['publicationdate'] = pd.to_datetime(avg_asking_price['publicationdate'])

    #create column month and year and create columns that will be used to group by
    avg_asking_price['month'] = avg_asking_price['publicationdate'].apply(lambda x: x.month)
    avg_asking_price['year'] = avg_asking_price['publicationdate'].apply(lambda x: x.year)
    groups = ['municipalityname', 'year','month']
    
    #group by selected columns. calcualte mean and safe as pandas Dataframe
    avg_asking_price_mean = avg_asking_price.groupby(by=groups).mean().reset_index()

    print("QUERY 1: AVERAGE ASKING PRICE PER MONTH PER MUNICIPALITY:")
    print(avg_asking_price_mean)


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')

#Query 2
def query_2():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    #2. Average asking price per bevolkingsdichtheid group or category (you might  have  to  discretize  this  variable)  for  each  gemeente  in  the  Netherlands
    #select needed columns from database and store them in a pandas dataframe
    executing_script = "SELECT sellingprice, municipalityCode, municipalityname, PopulationDensity FROM (SELECT sellingprice, municipalityCode, municipalityname, zipcode FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names) as funda_zip NATURAL LEFT JOIN demographic_info;"
    avg_asking_price_popdens = sqlio.read_sql_query(executing_script, conn)
    #defining 10 equal sized bins, spcified by the first and last value inside populationDensity
    avg_asking_price_popdens['bins'] = pd.cut(avg_asking_price_popdens['populationdensity'], 10)
    avg_asking_price_popdens_grouped = avg_asking_price_popdens.groupby(by=['municipalitycode', 'bins']).mean().reset_index().groupby(by=['bins']).mean()['sellingprice']
    
    print("AVERAGE ASKING PRICE PER POPULATIONDENSITY GROUP")
    print(avg_asking_price_popdens_grouped.head(50))


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')

#Query 3
def query_3():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    #Select needed columns from database and store them in a pandas dataframe
    sellingprice_per_municipality__per_citizen_ordered_table = "SELECT sellingPrice, MunicipalityName, MunicipalityCode, averageincomepercitizen FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names NATURAL LEFT JOIN (SELECT MunicipalityCode, averageincomepercitizen from housing_info) AS housing_info;"
    sellingprice_per_municipality__per_citizen = sqlio.read_sql_query(sellingprice_per_municipality__per_citizen_ordered_table, conn)
    
    #Select the avg asking price per municipality ranked by avg income 
    print(sellingprice_per_municipality__per_citizen.groupby(['municipalityname']).mean().sort_values('averageincomepercitizen', ascending=False)) 

    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')

#Query 4
def query_4():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    #4. for  every  gemeente  in  the  Netherlands  and  every  month  in  2018-2019: the percentage increase or decrease in the average house price in that gemeente compared to the previous month
    #use the already grouped dataframe
    executing_script = "SELECT sellingPrice, publicationDate, MunicipalityCode, MunicipalityName FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names;"
    avg_asking_price = sqlio.read_sql_query(executing_script, conn)
    avg_asking_price['publicationdate'] = pd.to_datetime(avg_asking_price['publicationdate'])

    #create column month and year and create columns that will be used to group by
    avg_asking_price['month'] = avg_asking_price['publicationdate'].apply(lambda x: x.month)
    avg_asking_price['year'] = avg_asking_price['publicationdate'].apply(lambda x: x.year)
    groups = ['municipalityname', 'year','month']
    
    #group by selected columns. calcualte mean and safe as pandas Dataframe
    avg_asking_price_mean = avg_asking_price.groupby(by=groups).mean().reset_index()

    avg_asking_price_mean = avg_asking_price_mean.reset_index().rename(columns={'index':'Index'})

    def rel_difference(row):
        if row.month == 1:
            rel_difference = 0
        else:
            rel_difference = ((row.sellingprice/ avg_asking_price_mean[avg_asking_price_mean.Index == (row.Index -1)].sellingprice.iloc[0])-1)*100
        return rel_difference    
    
    avg_asking_price_mean['rel_diff'] = avg_asking_price_mean.apply(lambda x: rel_difference(x), axis= 1)

    print('RELATIVE DIFFERENCE TO THE PREVIOUS MONTH PER GEMEENTE')
    print(avg_asking_price_mean)

    avg_asking_price_mean[avg_asking_price_mean.municipalityname=='Amsterdam'][['month','sellingprice']].set_index('month').plot()
    plt.title('Avergae Houseprice per month in Amsterdam for 2018')
    plt.show()


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')

#Query 5
def query_5():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    monthly_median_per_municipality_table = "SELECT sellingPrice, publicationDate, MunicipalityName FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names"
    monthly_median_per_municipality = sqlio.read_sql_query(monthly_median_per_municipality_table, conn)
    
    #Group by municipality and month
    #Set publicationdate to datetime_format
    monthly_median_per_municipality['publicationdate'] = pd.to_datetime(monthly_median_per_municipality['publicationdate'])
    
    #Calculate difference between the prices
    #Monthly median per municipality
    #print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median()) 
    #Difference between monthly median per municipality
    #print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median().diff()) 
    
    #Calculate absolute difference between the prices
    print(monthly_median_per_municipality.set_index("publicationdate").groupby(['municipalityname', pd.Grouper(freq='M')]).median().diff().abs())

    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')
query_5()

#Query 6
def query_6():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')

#Query 7
def query_7():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')

#Query_8
def query_8():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()

    return print('Done')

def correlation_analysis_nlp():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    SQL = "Select * FROM funda_nlp_analysis NATURAL JOIN funda"
    NLP_Analysis = sqlio.read_sql_query(SQL, conn)    
    correlations = NLP_Analysis[['sellingprice','sellingtime','descriptionlength','noun','adj','verb','adv','rel_noun','rel_adj','rel_verb','rel_adv','emails','urls','numbers','currency']].corr()
    print(correlations)
    most_used_word = NLP_Analysis.groupby(['lexeme_1']).count().sort_values('id',ascending=False)['id']
    print(most_used_word.head(50))

    #End connection
    cur.close()
    conn.close()


    return print('Done')

def write_own_sql_query():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    SQL = input('Write your SQL Query here: ')
    try: 
        Output = sqlio.read_sql_query(SQL, conn)
    except Exception as e:
        print(e)
    print(Output)

    #End connection
    cur.close()
    conn.close()


    return print('Output printed')

def create_aggregated_municipality_info_table():
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    executing_script = "SELECT sellingPrice, sellingTime, publicationDate, MunicipalityCode, MunicipalityName FROM funda NATURAL LEFT JOIN zipcodes NATURAL LEFT JOIN municipality_names;"
    avg_asking_price = sqlio.read_sql_query(executing_script, conn)
    avg_asking_price['publicationdate'] = pd.to_datetime(avg_asking_price['publicationdate'])

    #create column month and year and create columns that will be used to group by
    avg_asking_price['month'] = avg_asking_price['publicationdate'].apply(lambda x: x.month)
    avg_asking_price['year'] = avg_asking_price['publicationdate'].apply(lambda x: x.year)
    groups = ['municipalitycode', 'year','month']
    
    #group by selected columns. calcualte mean and safe as pandas Dataframe
    avg_asking_price_mean = avg_asking_price.groupby(by=groups).mean().reset_index().reset_index().rename(columns={'index':'Index'})

    def rel_difference(row):
        if row.month == 1:
            rel_difference = 0
        else:
            rel_difference = ((row.sellingprice/ avg_asking_price_mean[avg_asking_price_mean.Index == (row.Index -1)].sellingprice.iloc[0])-1)*100
        return rel_difference 

    def abs_difference(row):
        if row.month == 1:
            abs_difference = 0
        else:
            abs_difference = (row.sellingprice - avg_asking_price_mean[avg_asking_price_mean.Index == (row.Index -1)].sellingprice.iloc[0])
        return abs_difference    
    
    avg_asking_price_mean['rel_diff'] = avg_asking_price_mean.apply(lambda x: rel_difference(x), axis= 1)
    avg_asking_price_mean['abs_diff'] = avg_asking_price_mean.apply(lambda x: abs_difference(x), axis= 1)
    avg_asking_price_mean = avg_asking_price_mean.drop(columns='Index')

    db_tables = {'aggregated_municipality_info':avg_asking_price_mean}
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
    
    #fill tables one by one with info:
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

    

    #End connection
    cur.close()
    conn.close()

    return print('Done')


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
    4)numberrooms+sellingtime= 0.136939 5)numberbathrooms+sellingtime=-0.073602 6)surface+sellingtime=0.153849'''
    
    return print('Done')

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

def add_tourist_info_to_database():
    #Start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    tourist_info = pd.read_csv(os.path.join(root, 'Input_data/municipality code and National monuments 2018.csv', sep=';') 

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
    return print('Done')

def tourist_info_analysis():
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
    return print('Done')

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
    
    
    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Done')
    
if __name__ == '__main__':
    globals()[sys.argv[1]]()