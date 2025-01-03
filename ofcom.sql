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

FROM ofcom.nr_2022_2_tbl LIMIT 2;
.mode line

FROM ofcom.nr_2022_2_tbl LIMIT 2;
.mode duckbox
DESCRIBE ofcom.nr_2022_2_tbl;
-- get a summary of the data with key stats (only a subset displayed here for demo)
SUMMARIZE FROM ofcom.nr_2022_2_tbl LIMIT 100000;

-- create a geometry column from the lat/long columns
-- reprojecting to British National Grid to match with the grid data
ALTER TABLE ofcom.nr_2022_2_tbl ADD COLUMN geom GEOMETRY;

UPDATE ofcom.nr_2022_2_tbl 
SET geom = ST_Transform(ST_Point(latitude, longitude), 'EPSG:4326', 'EPSG:27700');

-- verify the geometry column
.mode line
SELECT geom FROM ofcom.nr_2022_2_tbl LIMIT 2;
.mode duckbox

-- clip (intersect) the point data to the grid data and save in a view
-- this assigns a grid square to each point
CREATE OR REPLACE VIEW labelled_point_vw AS
SELECT ofcom.os_gb_grids.plan_no
    , COLUMNS('rssi*|sinr*|pci*')
    , ST_Intersection(ofcom.nr_2022_2_tbl.geom, ofcom.os_gb_grids.geom) AS intersection
FROM ofcom.nr_2022_2_tbl , ofcom.os_gb_grids 
WHERE ST_Intersects(ofcom.nr_2022_2_tbl.geom, ofcom.os_gb_grids.geom);

FROM labelled_point_vw LIMIT 2;

-- calculate the grouped signal strength stats for each grid tile
-- and join back to the grid data to capture the grid geometry
CREATE OR REPLACE VIEW grid_stats_geom_vw AS
WITH cte_grid_stats AS
    (SELECT plan_no, MEDIAN(COLUMNS('rssi*|sinr*')), COLUMNS('pci*')
    FROM labelled_point_vw
    GROUP BY plan_no, pci_top1_3uk, pci_top1_ee, pci_top1_o2, pci_top1_vf)
-- now join the cte stats to the grid file, keeping only those that match    
SELECT ofcom.os_gb_grids.plan_no, geom, COLUMNS('rssi*|sinr*|pci*')
FROM ofcom.os_gb_grids
JOIN cte_grid_stats
ON ofcom.os_gb_grids.plan_no = cte_grid_stats.plan_no;

-- Introspect the view, filtering on columns that are not null
FROM grid_stats_geom_vw
WHERE (rssi_top1_3uk IS NOT NULL) AND (rssi_top1_ee IS NOT NULL)
LIMIT 2;

-- See if we can export to a modern GIS format like FlatGeoBuf
SELECT * FROM ST_Drivers()
WHERE short_name LIKE '%Flat%';

-- save the grid stats to a flatgeobuf file for use in QGIS
COPY (SELECT * FROM grid_stats_geom_vw) 
TO 'data/grid_stats_geom.fgb' 
WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');
.quit