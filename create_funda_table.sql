DROP TABLE IF EXISTS funda_2018;

--create the table again with plain text columns 
CREATE TABLE funda_2018(globalId integer PRIMARY KEY, publicatieDatum text, postcode text, koopPrijs text, volledigeOmschrijving text, soortWoning text, categorieObject text, bouwjaar text, indTuin text, perceelOppervlakte text, kantoor_naam_MD5hash text, aantalKamers text, aantalBadkamers text, energielabelKlasse text, globalId1 integer, oppervlakte text, datum_ondertekening text);

-- load the csv into the table
\copy funda_2018(globalId, publicatieDatum, postcode, koopPrijs, volledigeOmschrijving, soortWoning, categorieObject, bouwjaar, indTuin, perceelOppervlakte, kantoor_naam_MD5hash, aantalKamers, aantalBadkamers, energielabelKlasse, globalId1, oppervlakte, datum_ondertekening) FROM '/home/pi/RSL/Assignment_2/Input_Data/housing_data.csv' null 'NULL' CSV HEADER;

