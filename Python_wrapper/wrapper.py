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

    def category_to_list(item):
        item_list = item.split(' ')
        cleaned_item_list = []
        for i in item_list:
            cleaned_item = i.replace('<','').replace('{','').replace('>','').replace('}','').replace('(','').replace(')','')
            cleaned_item_list.append(cleaned_item)
        return str(cleaned_item_list)

    funda_cleaned['houseType'] = funda_cleaned['houseType'].apply(lambda x: category_to_list(x))
    funda_cleaned['categoryObject'] = funda_cleaned['categoryObject'].apply(lambda x: str(x).replace('<','').replace('{','').replace('>','').replace('}',''))
    #funda_cleaned['sellingDate'] = pd.to_datetime(funda_cleaned['sellingDate'])
    #funda_cleaned['publicationDate'] = pd.to_datetime(funda_cleaned['publicationDate'])
    funda_cleaned['sellingTime'] = pd.to_datetime(funda_cleaned['sellingDate']) - pd.to_datetime(funda_cleaned['publicationDate'])
    #funda_cleaned['sellingTime'] = funda_cleaned.sellingDate - funda_cleaned.publicationDate
    funda_cleaned['sellingTime'] = funda_cleaned['sellingTime'].apply(lambda x: int(x.days))
    funda_db = funda_cleaned

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
        cbs_cleaned = cbs_cleaned.astype({k: v})#,'PercentageInhabited':'int64','PercentageUninhabited': 'int64','OwnerOccupiedHouses': 'int64','RentalHouses': 'int64','ConstructionYearBefore2000 ': 'int64','ConstructionYearAfter2000': 'int64','AverageIncomePerCitizen': 'float64','MostCommonPostalCode': 'int64','CoveragePercentage ': 'int64'}, errors='ignore')
    
    #'PercentageInhabited': 'int64','PercentageUninhabited': 'int64','OwnerOccupiedHouses': 'int64','RentalHouses': 'int64','ConstructionYearBefore2000 ': 'int64','ConstructionYearAfter2000': 'int64','AverageIncomePerCitizen': 'float64','MostCommonPostalCode': 'int64','CoveragePercentage ': 'int64'
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

    print(postcodes_db.dtypes)
    print(buurt_names_db.dtypes)
    #specifiy tables to be created with their name and create them with the correct datatypes for postgres.
    db_tables = {'funda':funda_db,'demographic_info':demographic_info_db,'housing_info':housing_info_db,'municipality_names':municipality_names_db,'district_names':district_names_db,'Neighborhood_names':buurt_names_db,'zipcodes':postcodes_db}
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

def add_funda_data(csv_path, year):
    #start connection with database
    with open ('db_login.txt', 'r') as myfile:
        data = myfile.read()
    conn = psycopg2.connect(data)
    cur = conn.cursor()

    ######WRITE THE CODE HERE############################
    with open('cbs_cleaning.sql', 'r') as sql_file:
        commands = sql_file.read()
    for i in commands.split(';'):
        print((str(i.replace('\n',''))+';'))

    '''
    (1) load the funda data from csv, 
    (2) preprocess the data, 
    (3) connect to the database, 
    (4) upload the data into a database table for year 2018,
    '''


    ######################################################


    #Make changes to db persistent
    conn.commit()

    #End connection
    cur.close()
    conn.close()
    return print('Funda Data succesfully added')
#add_funda_data('/some/path', 2018)


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
    4)numberrooms+sellingtime= 0.136939 5)numberbathrooms+sellingtime=-0.073602 6)surface+sellingtime=0.153849'''
    
    return print('Analysis succesfully done')
correlation_housing_data_sellingprice_sellingtime()
