#%%
import duckdb

#%%

path1 = "data/nr_2023_1.csv" #9GB
path2 = "data/nr_2023_3.csv" #24GB
#%%
# schema translated from polars with claude AI
schema = {
    'latitude': 'DOUBLE',
    'longitude': 'DOUBLE',
    'month_year': 'VARCHAR',
    'hour_ref': 'INTEGER',
    'rnum': 'INTEGER',
    'pci_top1_3uk': 'INTEGER',
    'rssi_top1_3uk': 'DOUBLE',
    'ssb_idx_top1_3uk': 'INTEGER',
    'sinr_top1_3uk': 'VARCHAR',
    'rsrp_top1_3uk': 'VARCHAR',
    'rsrq_top1_3uk': 'VARCHAR',
    'arfcn_top1_3uk': 'INTEGER',
    'add_plmn_top1_3uk': 'VARCHAR',
    'mcc_top1_3uk': 'VARCHAR',
    'mnc_top1_3uk': 'VARCHAR',
    'nr_mode_top1_3uk': 'VARCHAR',
    'pci_top1_ee': 'INTEGER',
    'rssi_top1_ee': 'DOUBLE',
    'ssb_idx_top1_ee': 'INTEGER',
    'sinr_top1_ee': 'DOUBLE',
    'rsrp_top1_ee': 'DOUBLE',
    'rsrq_top1_ee': 'DOUBLE',
    'arfcn_top1_ee': 'INTEGER',
    'add_plmn_top1_ee': 'VARCHAR',
    'mcc_top1_ee': 'INTEGER',
    'mnc_top1_ee': 'INTEGER',
    'nr_mode_top1_ee': 'VARCHAR',
    'pci_top1_o2': 'INTEGER',
    'rssi_top1_o2': 'DOUBLE',
    'ssb_idx_top1_o2': 'INTEGER',
    'sinr_top1_o2': 'VARCHAR',
    'rsrp_top1_o2': 'VARCHAR',
    'rsrq_top1_o2': 'VARCHAR',
    'arfcn_top1_o2': 'INTEGER',
    'add_plmn_top1_o2': 'VARCHAR',
    'mcc_top1_o2': 'VARCHAR',
    'mnc_top1_o2': 'VARCHAR',
    'nr_mode_top1_o2': 'VARCHAR',
    'pci_top1_vf': 'INTEGER',
    'rssi_top1_vf': 'DOUBLE',
    'ssb_idx_top1_vf': 'INTEGER',
    'sinr_top1_vf': 'VARCHAR',
    'rsrp_top1_vf': 'VARCHAR',
    'rsrq_top1_vf': 'VARCHAR',
    'arfcn_top1_vf': 'INTEGER',
    'add_plmn_top1_vf': 'VARCHAR',
    'mcc_top1_vf': 'INTEGER',
    'mnc_top1_vf': 'INTEGER',
    'nr_mode_top1_vf': 'VARCHAR'
}

#%%
qry = f"""
SELECT * 
FROM read_csv('{path2}', columns = {schema})
"""
#%%
# use duckdb's relational API'
rel = duckdb.sql(qry)


#%%
%%time
(rel
 .filter("latitude > 51 AND latitude < 52 AND longitude > -3 AND longitude < -1")
 .count("*")
 .show()
)
#%%