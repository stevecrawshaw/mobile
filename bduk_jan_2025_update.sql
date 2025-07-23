duckdb

--INSTALL SPATIAL;
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

DESCRIBE weca_postgres.os.codepoint_open;

USE ofcom;
CREATE OR REPLACE TABLE ofcom.lep_postcode_geom_tbl AS
SELECT 
    postcode,
    admin_district_code ladcd,
    -- need to reverse order of vertices with ST_Reverse()!
    -- shape.ST_GeomFromWKB().ST_Transform('EPSG:27700', 'EPSG:4326').ST_Y() AS latitude
    -- manually create the geo_point_2d column as export via geojson fails
    geo_point_2d: '{' || lat::VARCHAR || ', ' || lon::VARCHAR || '}'
FROM weca_postgres.os.codepoint_open 
WHERE weca_postgres.os.codepoint_open.admin_district_code IN('E06000022', 'E06000023', 'E06000024', 'E06000025');

DETACH weca_postgres;

FROM ofcom.lep_postcode_geom_tbl LIMIT 2;

.tables

CREATE OR REPLACE TABLE ofcom.bduk_202501_sw_tbl AS
SELECT * FROM read_csv('data/202501_BDUK_uprn_release_south_west/*.csv');

.mode line
FROM ofcom.bduk_202501_sw_tbl LIMIT 1;
.mode duckbox

SELECT COUNT(*) FROM ofcom.bduk_202501_sw_tbl;


-- pivoted subsidy control status by postcode
CREATE OR REPLACE TABLE ofcom.subsidy_control_pivot_2025_tbl AS
WITH scs_cte AS(
PIVOT (FROM ofcom.bduk_202501_sw_tbl)
ON subsidy_control_status
USING COUNT(subsidy_control_status)
GROUP BY postcode, local_authority_district_ons_code, local_authority_district_ons)
SELECT * FROM scs_cte
WHERE local_authority_district_ons_code IN('E06000022', 'E06000023', 'E06000024', 'E06000025');

FROM ofcom.subsidy_control_pivot_2025_tbl LIMIT 2;

CREATE OR REPLACE FUNCTION stringlist(col) AS list(col).list_distinct().array_to_string(e'\n');


CREATE OR REPLACE TABLE ofcom.lep_bduk_ods_2025_tbl AS
SELECT 
    postcode,
    MAX(bduk_gis_final_coverage_date) as max_date,
    MIN(bduk_gis_final_coverage_date) as min_date,
    
    SUM(CASE
        WHEN current_gigabit 
        THEN 1 ELSE 0 END) * 100 / (COUNT(current_gigabit))::SMALLINT as current_gigabit_prop,
    SUM(CASE
        WHEN current_gigabit 
        THEN 1 ELSE 0 END)::SMALLINT  as current_gigabit_count,

    SUM(CASE
        WHEN future_gigabit 
        THEN 1 ELSE 0 END) * 100 / (COUNT(future_gigabit))::SMALLINT as future_gigabit_prop,
    SUM(CASE
        WHEN future_gigabit 
        THEN 1 ELSE 0 END)::SMALLINT  as future_gigabit_count,

    SUM(CASE
        WHEN bduk_gis 
        THEN 1 ELSE 0 END) * 100 / (COUNT(bduk_gis)) as bduk_gis_prop,
    SUM(CASE
        WHEN bduk_gis 
        THEN 1 ELSE 0 END)::SMALLINT  as bduk_gis_count,

    SUM(CASE
        WHEN bduk_vouchers 
        THEN 1 ELSE 0 END) * 100 / (COUNT(bduk_vouchers)) as bduk_vouchers_prop,
    SUM(CASE
        WHEN bduk_vouchers 
        THEN 1 ELSE 0 END)::SMALLINT  as bduk_vouchers_count,

        SUM(CASE
        WHEN bduk_hubs 
        THEN 1 ELSE 0 END) * 100 / (COUNT(bduk_hubs)) as bduk_hubs_prop,
    SUM(CASE
        WHEN bduk_hubs 
        THEN 1 ELSE 0 END)::SMALLINT  as bduk_hubs_count,

    -- aggregate suppliers for each postcode (there may be multiple suppliers)
    -- GIS
    stringlist(bduk_gis_supplier) gis_suppliers,
    stringlist(bduk_gis_contract_name) gis_contracts,
    -- VOUCHERS
    stringlist(bduk_vouchers_supplier) vouchers_suppliers,
    stringlist(bduk_vouchers_contract_name) vouchers_contracts,
    -- -- SUPERFAST
    stringlist(bduk_superfast_supplier) superfast_suppliers,
    stringlist(bduk_superfast_contract_name) superfast_contracts,
    -- -- HUBS
    stringlist(bduk_hubs_supplier) hubs_suppliers,
    stringlist(bduk_hubs_contract_name) hubs_contracts

FROM ofcom.bduk_202501_sw_tbl
GROUP BY postcode, local_authority_district_ons_code
HAVING 
   --max_date IS NOT NULL AND 
    local_authority_district_ons_code IN('E06000022', 'E06000023', 'E06000024', 'E06000025');


CREATE OR REPLACE VIEW ofcom.lep_bduk_ods_postcode_out_vw AS
SELECT * 
        EXCLUDE (local_authority_district_ons_code) 
        RENAME ("Gigabit Grey/Black" AS gigabit_gb,
                "Gigabit Under Review" AS gigabit_ur,
                "Gigabit White" AS gigabit_white)--bduk.*, geom.*, scs_piv.*
FROM lep_bduk_ods_2025_tbl bduk
INNER JOIN lep_postcode_geom_tbl geom
USING (postcode)
-- ON bduk.postcode = geom.postcode
INNER JOIN subsidy_control_pivot_2025_tbl scs_piv
USING (postcode);
-- ON bduk.postcode = scs_piv.postcode;

DESCRIBE ofcom.lep_bduk_ods_postcode_out_vw;

COPY ofcom.lep_bduk_ods_postcode_out_vw TO 'data/lep_bduk_ods_postcode_out.csv'
WITH (HEADER TRUE, DELIMITER ',');


.shell git add . && git commit -m '2025 data update' && git push origin main


.tables
.quit



-- postcode,
-- max_date,
-- min_date,
-- future_gigabit_prop "",
-- future_gigabit_count,
-- bduk_gis_prop,
-- bduk_gis_count,
-- bduk_vouchers_prop,
-- bduk_vouchers_count,
-- bduk_hubs_prop,
-- bduk_hubs_count,
-- suppliers,
-- contracts,
-- ladcd,
-- geo_point_2d,
-- local_authority_district_ons,
-- "Gigabit Grey/Black",
-- "Gigabit Under Review",
-- "Gigabit White"