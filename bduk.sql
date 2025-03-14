duckdb

-- install extensions
-- spatial functions like in POSTGIS
INSTALL SPATIAL;
LOAD SPATIAL;

ATTACH 'data/ofcom.duckdb' AS ofcom;
-- Connect to MCA's Postgres database VPN ON!!!!!
-- credentials are stored in a secret manager
ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

SELECT * FROM information_schema.tables WHERE table_schema = 'os';

.databases
.help
.tables

.tables %code%
.mode line

SELECT 
    postcode,
    admin_district_code ladcd,
    shape.ST_GeomFromWKB().ST_Transform('EPSG:27700', 'EPSG:4326') AS geom
FROM weca_postgres.os.codepoint_open 
WHERE admin_district_code IN('E06000022', 'E06000023', 'E06000024', 'E06000025')
LIMIT 1;


-- SELECT 
--     uprn,
--     postcode_locator,
-- FROM weca_postgres.os.adb_premium_lep;


.mode line
FROM read_csv('data/202409_BDUK_uprn_release_South_West.csv') LIMIT 1;

.mode duckbox
SELECT 
    postcode,
    MAX(gis_final_coverage_date) as max_date,
    MIN(gis_final_coverage_date) as min_date,
    
    SUM(CASE
        WHEN future_gigabit 
        THEN 1 ELSE 0 END) / (COUNT(future_gigabit)) as future_gigabit_prop,
FROM read_csv('data/202409_BDUK_uprn_release_South_West.csv')
GROUP BY postcode, local_authority_district_ons_code
HAVING 
   --max_date IS NOT NULL AND 
    local_authority_district_ons_code IN('E06000022', 'E06000023', 'E06000024', 'E06000025');