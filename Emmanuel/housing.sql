/*Here I created the table mymovies10 to load the data from csv file into it*/
CREATE TABLE Funda_Houses (
globalid1 text,
publicatiedatum text,
postcode text,
koopPrijs text,
volledigeOmschrijving text,
soortWoning text,
categorieObject text,
bouwjaar text,
indTuin text,
perceelOppervlakte text,
kantoor_naam_MD5hahs text,
aantalKamers text,
aantalBadkamers text,
energielabelKlasse text,
globalid2 text,
oppervlakte text,
datum_ondertekening text
);

/*copy the data from the funda csv file to the created table*/
COPY Funda_Houses(globalid1, publicatieDatum, postcode, koopPrijs, volledigeOmschrijving, soortWoning, categorieObject, bouwjaar, indTuin, perceelOppervlakte, kantoor_naam_MD5hahs, aantalKamers, aantalBadkamers, energielabelKlasse, globalid2, oppervlakte, datum_ondertekening) FROM '/home/pi/RSL/housing_data.csv' CSV HEADER;

