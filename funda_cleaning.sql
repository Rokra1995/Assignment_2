-- change the column names to English names
ALTER TABLE funda_2018 RENAME COLUMN publicatieDatum TO publicationDate;
ALTER TABLE funda_2018 RENAME COLUMN postcode TO zipcode;
ALTER TABLE funda_2018 RENAME COLUMN koopPrijs TO sellingPrice;
ALTER TABLE funda_2018 RENAME COLUMN volledigeOmschrijving TO fullDescription;
ALTER TABLE funda_2018 RENAME COLUMN soortWoning TO houseType;
ALTER TABLE funda_2018 RENAME COLUMN categorieObject TO categoryObject;
ALTER TABLE funda_2018 RENAME COLUMN bouwjaar TO yearOfBuilding;
ALTER TABLE funda_2018 RENAME COLUMN indTuin TO garden;
ALTER TABLE funda_2018 RENAME COLUMN perceelOppervlakte TO parcelSurface;
ALTER TABLE funda_2018 RENAME COLUMN aantalKamers TO numberRooms;
ALTER TABLE funda_2018 RENAME COLUMN aantalBadkamers TO numberBathrooms;
ALTER TABLE funda_2018 RENAME COLUMN energielabelKlasse TO energylabelClass;
ALTER TABLE funda_2018 RENAME COLUMN oppervlakte TO surface;
ALTER TABLE funda_2018 RENAME COLUMN datum_ondertekening TO sellingDate;

-- remove columns 
ALTER TABLE funda_2018 DROP COLUMN globalId1;
-- If Error, check if globalId1 is correctly named, it could be globalId_1
ALTER TABLE funda_2018 DROP COLUMN kantoor_naam_MD5hash;

-- set NULL strings to sql NULL values 
UPDATE funda_2018 SET sellingPrice = NULL WHERE sellingPrice = 'NULL';
UPDATE funda_2018 SET numberRooms = NULL WHERE numberRooms = 'NULL';
UPDATE funda_2018 SET numberBathrooms = NULL WHERE numberBathrooms = 'NULL';
UPDATE funda_2018 SET energylabelClass = NULL WHERE energylabelClass = 'NULL';
UPDATE funda_2018 SET parcelSurface = NULL WHERE parcelSurface = 'NULL';

-- update the data type to integer
ALTER TABLE funda_2018 ALTER COLUMN sellingPrice TYPE integer USING sellingPrice::integer;
ALTER TABLE funda_2018 ALTER COLUMN numberRooms TYPE integer USING numberRooms::integer;
ALTER TABLE funda_2018 ALTER COLUMN numberBathrooms TYPE integer USING numberBathrooms::integer;
ALTER TABLE funda_2018 ALTER COLUMN parcelSurface TYPE integer USING parcelSurface::integer;

-- update the data type to date
ALTER TABLE funda_2018 ALTER COLUMN publicationDate TYPE date USING publicationDate::date;
ALTER TABLE funda_2018 ALTER COLUMN sellingDate TYPE date USING sellingDate::date;

-- Add column with the selling time
ALTER TABLE funda_2018 ADD COLUMN sellingTime integer GENERATED ALWAYS AS (sellingDate - publicationDate) STORED;

-- add column with the length of the description
ALTER TABLE funda_2018 ADD COLUMN descriptionLength integer GENERATED ALWAYS AS (array_length(regexp_split_to_array(fulldescription, E'\\s+'),1)) STORED;