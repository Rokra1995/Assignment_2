--Rename columns to english translation
ALTER TABLE neighborhood RENAME COLUMN buurtcode2020 TO NeighborhoodKey;
ALTER TABLE neighborhood RENAME COLUMN buurtnaam2020 TO NeighborhoodName;
ALTER TABLE neighborhood RENAME COLUMN GM_2020 TO MunicipalityCode;
ALTER TABLE neighborhood RENAME COLUMN GM2020 TO MunicipalityKey;
ALTER TABLE neighborhood RENAME COLUMN GM_NAAM2020 TO MunicipalityName;
ALTER TABLE neighborhood RENAME COLUMN WK_2020 TO DistrictCode;
ALTER TABLE neighborhood RENAME COLUMN WK2020 TO DistrictKey;
ALTER TABLE neighborhood RENAME COLUMN WK_NAAM TO DistrictName;

-- RENAME TABLE NEIGBORHOOD_INFO
ALTER TABLE neighborhood_info RENAME COLUMN WijkenEnBuurten TO NeighborhoodsAndDistricts;
ALTER TABLE neighborhood_info RENAME COLUMN Gemeentenaam_1 TO NameOfMunicipality;
ALTER TABLE neighborhood_info DROP COLUMN Codering_3;
ALTER TABLE neighborhood_info RENAME COLUMN Mannen_6 TO NumberOfMens;
ALTER TABLE neighborhood_info RENAME COLUMN Vrouwen_7 TO NumberOfWomen;
ALTER TABLE neighborhood_info RENAME COLUMN k_0Tot15Jaar_8 TO AgeFrom0to15years;
ALTER TABLE neighborhood_info RENAME COLUMN k_15Tot25Jaar_9 TO AgeFrom15to25years;
ALTER TABLE neighborhood_info RENAME COLUMN k_25Tot45Jaar_10 TO AgeFrom25to45years;
ALTER TABLE neighborhood_info RENAME COLUMN k_45Tot65Jaar_11 TO AgeFrom45to65years;
ALTER TABLE neighborhood_info RENAME COLUMN k_65JaarOfOuder_12 TO AgeFrom65AndOlder;
ALTER TABLE neighborhood_info RENAME COLUMN Bevolkingsdichtheid_33 TO PopulationDensity;
ALTER TABLE neighborhood_info RENAME COLUMN Woningvoorraad_34 TO HousingStock;
ALTER TABLE neighborhood_info RENAME COLUMN PercentageBewoond_38 TO PercentageInhabited;
ALTER TABLE neighborhood_info RENAME COLUMN PercentageBewoond_39 TO PercentageUninhabited;
ALTER TABLE neighborhood_info RENAME COLUMN Koopwoningen_40 TO OwnerOccupiedHouses;
ALTER TABLE neighborhood_info RENAME COLUMN HuurwoningenTotaal_41 TO RentalHouses;
ALTER TABLE neighborhood_info RENAME COLUMN BouwjaarVoor2000_45 TO ConstructionYearBefore2000;
ALTER TABLE neighborhood_info RENAME COLUMN BouwjaarVanaf2000_46 TO ConstructionYearAfter2000;
ALTER TABLE neighborhood_info RENAME COLUMN GemiddeldInkomenPerInwoner_66 TO AverageIncomePerCitizen;
ALTER TABLE neighborhood_info RENAME COLUMN MeestVoorkomendePostcode_103 TO MostCommonPostalCode;
ALTER TABLE neighborhood_info RENAME COLUMN Dekkingspercentage_104 TO CoveragePercentage;

-- RENAME TABLE MUNICIPALITY_INFO
ALTER TABLE municipality_names RENAME COLUMN Gemcode2020 TO MunicipalityKey;
ALTER TABLE municipality_names RENAME COLUMN Gemeentenaam2020 TO MunicipalityName;

-- RENAME TABLE DISTRICT_NAMES
ALTER TABLE district_names RENAME COLUMN wijkcode2020 TO DistrictKey;
ALTER TABLE district_names RENAME COLUMN wijknaam2020 TO DistrictName;

-- RENAME TABLE POSTCODES
ALTER TABLE postcodes RENAME COLUMN PC6 TO Postcode;
ALTER TABLE postcodes RENAME COLUMN Buurt2020 TO NeighborhoodKey;
ALTER TABLE postcodes RENAME COLUMN Wijk2020 TO DistrictKey;
ALTER TABLE postcodes RENAME COLUMN Gemeente2020 TO MunicipalityKey;

