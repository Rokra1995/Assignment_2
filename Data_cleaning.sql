-- change the column names to English names
ALTER TABLE funda RENAME COLUMN publicatieDatum TO publicationDate;
ALTER TABLE funda RENAME COLUMN postcode TO zipcode;
ALTER TABLE funda RENAME COLUMN koopPrijs TO sellingPrice;
ALTER TABLE funda RENAME COLUMN volledigeOmschrijving TO fullDescription;
ALTER TABLE funda RENAME COLUMN soortWoning TO houseType;
ALTER TABLE funda RENAME COLUMN categorieObject TO categoryObject;
ALTER TABLE funda RENAME COLUMN bouwjaar TO yearOfBuilding;
ALTER TABLE funda RENAME COLUMN indTuin TO garden;
ALTER TABLE funda RENAME COLUMN perceelOppervlakte TO parcelSurface;
ALTER TABLE funda RENAME COLUMN aantalKamers TO numberRooms;
ALTER TABLE funda RENAME COLUMN aantalBadkamers TO numberBathrooms;
ALTER TABLE funda RENAME COLUMN energielabelKlasse TO energylabelClass;
ALTER TABLE funda RENAME COLUMN oppervlakte TO surface;
ALTER TABLE funda RENAME COLUMN datum_ondertekening TO sellingDate;

-- remove columns 
ALTER TABLE funda DROP COLUMN globalId1;
-- If Error, check if globalId1 is correctly named, it could be globalId_1
ALTER TABLE funda DROP COLUMN kantoor_naam_MD5hash;

-- set NULL strings to sql NULL values 
UPDATE funda SET sellingPrice = NULL WHERE sellingPrice = 'NULL';
UPDATE funda SET numberRooms = NULL WHERE numberRooms = 'NULL';
UPDATE funda SET numberBathrooms = NULL WHERE numberBathrooms = 'NULL';
UPDATE funda SET energylabelClass = NULL WHERE energylabelClass = 'NULL';
UPDATE funda SET parcelSurface = NULL WHERE parcelSurface = 'NULL';

-- update the data type to integer
ALTER TABLE funda ALTER COLUMN sellingPrice TYPE integer USING sellingPrice::integer;
ALTER TABLE funda ALTER COLUMN numberRooms TYPE integer USING numberRooms::integer;
ALTER TABLE funda ALTER COLUMN numberBathrooms TYPE integer USING numberBathrooms::integer;
ALTER TABLE funda ALTER COLUMN parcelSurface TYPE integer USING parcelSurface::integer;

-- update the data type to date
ALTER TABLE funda ALTER COLUMN publicationDate TYPE date USING publicationDate::date;
ALTER TABLE funda ALTER COLUMN sellingDate TYPE date USING sellingDate::date;

-- Add column with the selling time
ALTER TABLE funda ADD COLUMN sellingTime integer GENERATED ALWAYS AS (sellingDate - publicationDate) STORED;

-- add column with the length of the description
ALTER TABLE funda ADD COLUMN descriptionLength integer GENERATED ALWAYS AS array_length(regexp_split_to_array(fullDescription, '\\s+'),1) STORED;
