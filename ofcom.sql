-- Use duckdb to wrangle large data files from OFCOM which hold signal strength
-- data for the UK. The data is in CSV format and is larger than memory (~24GB per file). 
-- The end result will be a GIS data file which represents median signal strength from all
-- mobile operators at a 1km grid square resolution for the roads driven.

./duckdb

ATTACH 'data/ofcom.duckdb' AS ofcom;
-- install extensions
-- spatial functions like in POSTGIS
INSTALL SPATIAL;
LOAD SPATIAL;

-- Connect to MCA's Postgres database through the postgres extension
INSTALL postgres;
LOAD postgres;

ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);
.tables

FROM weca_postgres.os.grid_1km LIMIT 2;

-- convert the hex string to a geometry object
-- and filter to only the grid squares in ST grids, which cover our region

SELECT plan_no, shape.from_hex().ST_GeomFromWKB() AS geom
FROM weca_postgres.os.grid_1km
WHERE plan_no LIKE 'ST%'
LIMIT 2;

-- https://github.com/OrdnanceSurvey/OS-British-National-Grids
-- unzip with py7zr
-- introspect the grid geopackage to get the layer names - we want the 1km grid
ATTACH 'data/os_bng_grids.gpkg' AS os_grids_db (TYPE SQLITE);
USE os_grids_db;

.tables
-- the layer we want is '1km_grid'

USE ofcom;
DETACH os_grids_db;

CREATE TABLE os_gb_grids AS SELECT * FROM ST_Read('data/os_bng_grids.gpkg', layer = '1km_grid');
FROM os_gb_grids;

-- how many rows in the CSV?
SELECT COUNT(*) FROM read_csv_auto('data/nr_2022_2.csv');

-- create persistent storage for the data within the region
-- using a bounding box filter
CREATE TABLE nr_2022_2_tbl AS 
SELECT * FROM read_csv_auto('data/nr_2022_2.csv')
WHERE 
(latitude BETWEEN 51.270 AND 51.9) 
AND 
(longitude BETWEEN -3.021 AND -2.252);

.mode line

FROM nr_2022_2_tbl LIMIT 2;

.mode duckbox
FROM nr_2022_2_tbl;
DESCRIBE nr_2022_2_tbl;

SUMMARIZE nr_2022_2_tbl;

-- create a geometry column from the lat/long columns
-- reprojecting to British National Grid to match with the grid data
ALTER TABLE nr_2022_2_tbl ADD COLUMN geom GEOMETRY;

UPDATE nr_2022_2_tbl SET geom = ST_Transform(ST_Point(latitude, longitude), 'EPSG:4326', 'EPSG:27700');

.mode line
SELECT geom FROM  nr_2022_2_tbl LIMIT 2;
.mode duckbox

-- clip (intersect) the point data to the grid data and save in a view
CREATE OR REPLACE VIEW labelled_point_vw AS
SELECT os_gb_grids.tile_name
    , COLUMNS('rssi*|sinr*|pci*')
    , ST_Intersection(nr_2022_2_tbl.geom, os_gb_grids.geom) AS intersection
FROM nr_2022_2_tbl , os_gb_grids 
WHERE ST_Intersects(nr_2022_2_tbl.geom, os_gb_grids.geom);

FROM labelled_point_vw LIMIT 2;

-- calculate the grouped signal strength stats for each grid tile
-- and join back to the grid data to capture the grid geometry
CREATE OR REPLACE VIEW grid_stats_geom_vw AS
WITH cte_grid_stats AS
    (SELECT tile_name, MEDIAN(COLUMNS('rssi*|sinr*')), COLUMNS('pci*')
    FROM labelled_point_vw
    GROUP BY tile_name, pci_top1_3uk, pci_top1_ee, pci_top1_o2, pci_top1_vf)
-- now join the cte stats to the grid file, keeping only those that match    
SELECT os_gb_grids.tile_name, geom, COLUMNS('rssi*|sinr*|pci*')
FROM os_gb_grids
JOIN cte_grid_stats
ON os_gb_grids.tile_name = cte_grid_stats.tile_name;

-- export to a GIS compatible file
SELECT * FROM ST_Drivers()
WHERE short_name LIKE '%Flat%';

-- save the grid stats to a flatgeobuf file for use in QGIS
COPY (SELECT * FROM grid_stats_geom_vw) 
TO 'data/grid_stats_geom.fgb' 
WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');


.quit