DROP TABLE IF EXISTS neighborhood;
DROP TABLE IF EXISTS neighborhood_info;
DROP TABLE IF EXISTS municipality_names;
DROP TABLE IF EXISTS district_names;
DROP TABLE IF EXISTS postcodes;
DROP TABLE IF EXISTS neighborhood_names;

-- Create the neighborhood table and import the data
CREATE TABLE neighborhood(buurtcode2020 integer, buurtnaam2020 text, GM_2020 text, GM2020 integer, GM_NAAM text, WK_2020 text, WK2020 integer, WK_NAAM text);
\copy neighborhood(buurtcode2020, buurtnaam2020, GM_2020, GM2020, GM_NAAM, WK_2020, WK2020, WK_NAAM) FROM '/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/Input_data/brt2020.csv' delimiter ';' CSV HEADER;

-- create the neigbourhood information table and import the data
CREATE TABLE cbs_info(ID text, WijkenEnBuurten text, Gemeentenaam_1 text, Codering_3 text, Mannen_6 text, Vrouwen_7 text, k_0Tot15Jaar_8 text, k_15Tot25Jaar_9 text, k_25Tot45Jaar_10 text, k_45Tot65Jaar_11 text, k_65JaarOfOuder_12 text, Bevolkingsdichtheid_33 text, Woningvoorraad_34 text, PercentageBewoond_38 text, PercentageOnbewoond_39 text, Koopwoningen_40 text, HuurwoningenTotaal_41 text, BouwjaarVoor2000_45 text, BouwjaarVanaf2000_46 text, GemiddeldInkomenPerInwoner_66 text, MeestVoorkomendePostcode_103 text, Dekkingspercentage_104 text);
\copy cbs_info(ID, WijkenEnBuurten, Gemeentenaam_1, Codering_3, Mannen_6, Vrouwen_7, k_0Tot15Jaar_8, k_15Tot25Jaar_9, k_25Tot45Jaar_10, k_45Tot65Jaar_11, k_65JaarOfOuder_12, Bevolkingsdichtheid_33, Woningvoorraad_34, PercentageBewoond_38, PercentageOnbewoond_39, Koopwoningen_40, HuurwoningenTotaal_41, BouwjaarVoor2000_45, BouwjaarVanaf2000_46, GemiddeldInkomenPerInwoner_66, MeestVoorkomendePostcode_103, Dekkingspercentage_104) FROM '/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/Input_data/cbs_data.csv' delimiter ';' CSV HEADER; 

-- Create the municipality_names table and import the data
CREATE TABLE municipality_names(Gemcode2020 integer PRIMARY KEY, Gemeentenaam2020 text);
\copy municipality_names(Gemcode2020, Gemeentenaam2020) FROM '/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/Input_data/gem2020.csv' delimiter ';' CSV HEADER;

--Create the dirstrict names table and import data
CREATE TABLE district_names(wijkcode2020 integer PRIMARY KEY, wijknaam2020 text);
\copy district_names(wijkcode2020, wijknaam2020) FROM '/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/Input_data/wijk2020.csv' delimiter ';' CSV HEADER;

--Create the postcode table and import data
CREATE TABLE postcodes(PC6 text, Buurt2020 integer, Wijk2020 integer, Gemeente2020 integer);
\copy postcodes(PC6, Buurt2020, Wijk2020, Gemeente2020) FROM '/Users/Robinkratschmayr2/Library/Mobile Documents/com~apple~CloudDocs/2. Ausbildung/Master/Quarter 1/Database Management and Digital Tools/Assignment2/Input_data/pc6-gwb2020.csv' delimiter ';' CSV HEADER;