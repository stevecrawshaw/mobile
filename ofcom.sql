-- Use duckdb to wrangle large data files from OFCOM which hold signal strength
-- data for the UK. The data is in CSV format and is larger than memory (~24GB per file). 

-- The end result will be a GIS data file which represents median signal strength from all
-- mobile operators at a 1km grid square resolution for the roads driven.

-- start duckdb
./duckdb

-- install extensions
-- spatial functions like in POSTGIS
INSTALL SPATIAL;
LOAD SPATIAL;

ATTACH 'data/ofcom.duckdb' AS ofcom;
-- Connect to MCA's Postgres database VPN ON!!!!!
-- credentials are stored in a secret manager
ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

.databases
.help 
.tables

FROM weca_postgres.os.grid_1km LIMIT 2;

-- convert the hex string to a geometry object
-- and filter to only the grid squares in ST grids - [South West England], which cover our region

SELECT plan_no, shape.from_hex().ST_GeomFromWKB() AS geom
FROM weca_postgres.os.grid_1km
WHERE plan_no LIKE 'ST%'
LIMIT 2;

-- Now create a table to store the grid data
-- duckdb supports function chaining like in python libraries
CREATE TABLE IF NOT EXISTS ofcom.os_gb_grids AS
SELECT plan_no, shape.from_hex().ST_GeomFromWKB() AS geom
FROM weca_postgres.os.grid_1km
WHERE plan_no LIKE 'ST%';

-- Now lets look at the signal strength data
-- how many (million) rows in the CSV?
SELECT COUNT(*)
.round(-6)
.fdiv(1e6) AS rows_million
FROM read_csv_auto('data/nr_2022_2.csv');

-- create persistent storage for the data within the region
-- using a bounding box filter
-- the source data has lat \ long columns
CREATE TABLE ofcom.nr_2022_2_tbl AS 
SELECT * FROM read_csv_auto('data/nr_2022_2.csv')
WHERE 
(latitude BETWEEN 51.270 AND 51.9) 
AND 
(longitude BETWEEN -3.021 AND -2.252);

SELECT COUNT() FROM ofcom.nr_2022_2_tbl;

FROM ofcom.nr_2022_2_tbl LIMIT 2;
.mode line

FROM ofcom.nr_2022_2_tbl
USING SAMPLE 1%
LIMIT 2;

.mode duckbox
DESCRIBE ofcom.nr_2022_2_tbl;
-- get a summary of the data with key stats (only a subset displayed here for demo)

EXPLAIN ANALYZE 
SUMMARIZE 
FROM ofcom.nr_2022_2_tbl 
LIMIT 1000000;

-- create a geometry column from the lat/long columns
-- reprojecting to British National Grid to match with the grid data
ALTER TABLE ofcom.nr_2022_2_tbl ADD COLUMN geom GEOMETRY;

UPDATE ofcom.nr_2022_2_tbl 
SET geom = ST_Transform(ST_Point(latitude, longitude), 'EPSG:4326', 'EPSG:27700');

-- verify the geometry column
SELECT geom FROM ofcom.nr_2022_2_tbl LIMIT 2;

-- spatial join and summarise the signal strength data by grid square in one step
CREATE OR REPLACE VIEW grid_stats_geom_vw AS
SELECT plan_no, MEDIAN(COLUMNS('rssi*|sinr*')),  os_gb_grids.geom
FROM ofcom.nr_2022_2_tbl , ofcom.os_gb_grids 
WHERE st_within(ofcom.nr_2022_2_tbl.geom, ofcom.os_gb_grids.geom)
GROUP BY ALL;

-- Introspect the view, filtering on columns that are not null

FROM grid_stats_geom_vw
WHERE (rssi_top1_3uk IS NOT NULL) AND (rssi_top1_ee IS NOT NULL)
LIMIT 2;

-- See if we can export to a modern GIS format like FlatGeoBuf
SELECT * FROM ST_Drivers()
WHERE short_name LIKE '%Geo%';

-- save the grid stats to a flatgeobuf file for use in QGIS
COPY (SELECT * FROM grid_stats_geom_vw) 
TO 'data/grid_stats_geom.fgb' 
WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');

-- open QGIS to verify the data
.quit