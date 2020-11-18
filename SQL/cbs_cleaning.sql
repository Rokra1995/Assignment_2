--Rename columns to english translation
ALTER TABLE neighborhood RENAME COLUMN buurtcode2020 TO NeighborhoodKey;
ALTER TABLE neighborhood RENAME COLUMN buurtnaam2020 TO NeighborhoodName;
ALTER TABLE neighborhood RENAME COLUMN GM_2020 TO MunicipalityCode;
ALTER TABLE neighborhood RENAME COLUMN GM2020 TO MunicipalityKey;
ALTER TABLE neighborhood RENAME COLUMN GM_NAAM TO MunicipalityName;
ALTER TABLE neighborhood RENAME COLUMN WK_2020 TO DistrictCode;
ALTER TABLE neighborhood RENAME COLUMN WK2020 TO DistrictKey;
ALTER TABLE neighborhood RENAME COLUMN WK_NAAM TO DistrictName;

-- RENAME TABLE NEIGBORHOOD_INFO
ALTER TABLE cbs_info RENAME COLUMN WijkenEnBuurten TO NeighborhoodsAndDistricts;
ALTER TABLE cbs_info RENAME COLUMN Gemeentenaam_1 TO NameOfMunicipality;
ALTER TABLE cbs_info DROP COLUMN Codering_3;
ALTER TABLE cbs_info RENAME COLUMN Mannen_6 TO NumberOfMen;
ALTER TABLE cbs_info RENAME COLUMN Vrouwen_7 TO NumberOfWomen;
ALTER TABLE cbs_info RENAME COLUMN k_0Tot15Jaar_8 TO AgeFrom0to15years;
ALTER TABLE cbs_info RENAME COLUMN k_15Tot25Jaar_9 TO AgeFrom15to25years;
ALTER TABLE cbs_info RENAME COLUMN k_25Tot45Jaar_10 TO AgeFrom25to45years;
ALTER TABLE cbs_info RENAME COLUMN k_45Tot65Jaar_11 TO AgeFrom45to65years;
ALTER TABLE cbs_info RENAME COLUMN k_65JaarOfOuder_12 TO AgeFrom65AndOlder;
ALTER TABLE cbs_info RENAME COLUMN Bevolkingsdichtheid_33 TO PopulationDensity;
ALTER TABLE cbs_info RENAME COLUMN Woningvoorraad_34 TO HousingStock;
ALTER TABLE cbs_info RENAME COLUMN PercentageBewoond_38 TO PercentageInhabited;
ALTER TABLE cbs_info RENAME COLUMN PercentageOnBewoond_39 TO PercentageUninhabited;
ALTER TABLE cbs_info RENAME COLUMN Koopwoningen_40 TO OwnerOccupiedHouses;
ALTER TABLE cbs_info RENAME COLUMN HuurwoningenTotaal_41 TO RentalHouses;
ALTER TABLE cbs_info RENAME COLUMN BouwjaarVoor2000_45 TO ConstructionYearBefore2000;
ALTER TABLE cbs_info RENAME COLUMN BouwjaarVanaf2000_46 TO ConstructionYearAfter2000;
ALTER TABLE cbs_info RENAME COLUMN GemiddeldInkomenPerInwoner_66 TO AverageIncomePerCitizen;
ALTER TABLE cbs_info RENAME COLUMN MeestVoorkomendePostcode_103 TO MostCommonPostalCode;
ALTER TABLE cbs_info RENAME COLUMN Dekkingspercentage_104 TO CoveragePercentage;

-- RENAME TABLE MUNICIPALITY_INFO
ALTER TABLE municipality_names RENAME COLUMN Gemcode2020 TO MunicipalityKey;
ALTER TABLE municipality_names RENAME COLUMN Gemeentenaam2020 TO MunicipalityName;

-- RENAME TABLE DISTRICT_NAMES
ALTER TABLE district_names RENAME COLUMN wijkcode2020 TO DistrictKey;
ALTER TABLE district_names RENAME COLUMN wijknaam2020 TO DistrictName;

-- RENAME TABLE POSTCODES
ALTER TABLE postcodes RENAME COLUMN PC6 TO zipcode;
ALTER TABLE postcodes RENAME COLUMN Buurt2020 TO NeighborhoodKey;
ALTER TABLE postcodes RENAME COLUMN Wijk2020 TO DistrictKey;
ALTER TABLE postcodes RENAME COLUMN Gemeente2020 TO MunicipalityKey;


--Split cbs_info into 3 levels, municipality, district and Neighborhood:
CREATE TABLE IF NOT EXISTS municipality_info AS
  (SELECT * FROM cbs_info WHERE NeighborhoodsAndDistricts LIKE 'GM%');

CREATE TABLE IF NOT EXISTS district_info AS
  (SELECT * FROM cbs_info WHERE NeighborhoodsAndDistricts LIKE 'WK%');

CREATE TABLE IF NOT EXISTS neighborhood_info AS
  (SELECT * FROM cbs_info WHERE NeighborhoodsAndDistricts LIKE 'BU%');

ALTER TABLE municipality_info RENAME COLUMN NeighborhoodsAndDistricts TO MunicipalityCode;
ALTER TABLE district_info RENAME COLUMN NeighborhoodsAndDistricts TO DistrictCode;
ALTER TABLE neighborhood_info RENAME COLUMN NeighborhoodsAndDistricts TO NeighborhoodCode;

CREATE TABLE IF NOT EXISTS zipcodes AS (SELECT zipcode, MunicipalityCode, MunicipalityKey, DistrictCode, DistrictKey, NeighborhoodKey FROM postcodes NATURAL JOIN neighborhood);

CREATE TABLE IF NOT EXISTS neighborhood_names AS (SELECT DISTINCT NeighborhoodKey, NeighborhoodName FROM neighborhood);
DROP TABLE neighborhood;

ALTER TABLE district_names RENAME TO district_names_backup;
ALTER TABLE municipality_names RENAME TO municipality_names_backup;

CREATE TABLE IF NOT EXISTS district_names AS (SELECT DISTINCT DistrictCode, DistrictName FROM district_names_backup NATURAL JOIN zipcodes);
CREATE TABLE IF NOT EXISTS municipality_names AS (SELECT DISTINCT MunicipalityCode, MunicipalityName FROM municipality_names_backup NATURAL JOIN zipcodes);

DROP TABLE postcodes;
DROP TABLE district_names_backup;
DROP TABLE municipality_names_backup;

ALTER TABLE zipcodes DROP COLUMN MunicipalityKey;
ALTER TABLE zipcodes DROP COLUMN DistrictKey;

update municipality_info set municipalityCode = replace(municipalityCode, ' ','');
update neighborhood_info set neighborhoodCode = replace(neighborhoodCode, ' ','');
update district_info set districtCode = replace(districtCode, ' ','');


--Foreignkey example
-- ALTER TABLE orders
--    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers (id);


-- ADD Primary key example

--when the primary key already exists in table
--ALTER TABLE <table_name> ADD PRIMARY KEY (id);

--when the primary key not exists yet:
--ALTER TABLE test1 ADD COLUMN id SERIAL PRIMARY KEY;
 
 