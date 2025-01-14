-- script to create ODS datasets from OFCOM data on broadband coverage
-- uses the postcodes_tl lookup data from the CA EPC database to 
-- join the OFCOM data on postcodes to the Local Authority Districts

./duckdb

ATTACH '../mca-data/data/ca_epc.duckdb' AS ca_epc;

ATTACH 'data/ofcom_tom/ofcom_ods.duckdb' AS ofcom_ods;

.mode duckbox
 
-- get postcode and LA data for the lep from the ca_epc database
CREATE OR REPLACE TABLE ofcom_ods.postcode_lep_tbl AS
SELECT pc.pcds, pc.lat, pc.long, pc.oslaua, ca.ladnm 
FROM ca_epc.postcodes_tbl pc
INNER JOIN ca_epc.ca_la_tbl ca
ON pc.oslaua = ca.ladcd
WHERE ca.cauthnm = 'West of England';

-- distinct postcodes 2 letters in WOE
-- for use in selecting the files to import
SELECT DISTINCT pcds[1:2]
FROM ca_epc.postcodes_tbl
WHERE oslaua IN (SELECT ladcd FROM ca_epc.ca_la_tbl
WHERE cauthnm = 'West of England');

-- get the coverage data form the csv files as a single table
CREATE OR REPLACE TABLE ofcom_ods.fixed_pc_coverage_postcode_lep_tbl AS
SELECT * FROM 
read_csv_auto(['data/ofcom_tom/cn202407_postcode_files/postcode_files/202407_fixed_pc_coverage_r01_BA.csv',
            'data/ofcom_tom/cn202407_postcode_files/postcode_files/202407_fixed_pc_coverage_r01_SN.csv',
            'data/ofcom_tom/cn202407_postcode_files/postcode_files/202407_fixed_pc_coverage_r01_BS.csv',
            'data/ofcom_tom/cn202407_postcode_files/postcode_files/202407_fixed_pc_coverage_r01_GL.csv'],
            union_by_name = true
);

-- get the gigabit readiness data from LA based csv files

CREATE OR REPLACE TABLE ofcom_ods.subsidy_control_uprn_lep_tbl AS
SELECT * FROM 
read_csv_auto(['data/ofcom_tom/South_West/Bath_and_North_East_Somerset.csv',
            'data/ofcom_tom/South_West/Bristol_City_of.csv',
            'data/ofcom_tom/South_West/North_Somerset.csv',
            'data/ofcom_tom/South_West/South_Gloucestershire.csv'],
            union_by_name = true
);

DETACH ca_epc;
.tables

DESCRIBE ofcom_ods.subsidy_control_uprn_lep_tbl;

-- to create a view from pivot the values must be explicitly mentioned
CREATE OR REPLACE VIEW ofcom_ods.subsidy_control_postcode_wide_lep_view AS
PIVOT ofcom_ods.subsidy_control_uprn_lep_tbl
ON subsidy_control_status
IN ('Gigabit Grey/Black','Gigabit Under Review', 'Gigabit White')
USING count(UPRN)
GROUP BY Postcode, Local_Authority_District, Lot_Name
ORDER BY Local_Authority_District;


-- these are the two coverage tables with postcodes
-- now we join them with the postcode_lep_tbl

FROM ofcom_ods.subsidy_control_postcode_wide_lep_view;

FROM ofcom_ods.fixed_pc_coverage_postcode_lep_tbl;

-- create views for the ODS datasets and export to CSV for upload

CREATE OR REPLACE VIEW ofcom_ods.subsidy_control_postcode_wide_lep_ODS_view AS
SELECT *, '{' || lat || ', ' || long || '}' AS geo_point_2d
FROM ofcom_ods.postcode_lep_tbl pc
INNER JOIN ofcom_ods.subsidy_control_postcode_wide_lep_view sc
ON pc.pcds = sc.Postcode;

CREATE OR REPLACE VIEW ofcom_ods.fixed_pc_coverage_postcode_lep_ODS_view AS
SELECT *, '{' || lat || ', ' || long || '}' AS geo_point_2d
FROM ofcom_ods.postcode_lep_tbl pc
INNER JOIN ofcom_ods.fixed_pc_coverage_postcode_lep_tbl fc
ON pc.pcds = fc.postcode_space;

COPY ofcom_ods.subsidy_control_postcode_wide_lep_ODS_view
TO 'data/ofcom_tom/subsidy_control_postcode_wide_lep_ODS_view.csv'
(DELIMITER ',', HEADER true);

COPY ofcom_ods.fixed_pc_coverage_postcode_lep_ODS_view
TO 'data/ofcom_tom/fixed_pc_coverage_postcode_lep_ODS_view.csv'
(DELIMITER ',', HEADER true);

.quit