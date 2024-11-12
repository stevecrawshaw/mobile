#%%
import duckdb
import polars as pl
#%%
con = duckdb.connect(database="data/mobile.duckdb", read_only=False)

#%%
qry = """
CREATE OR REPLACE TABLE mobile_raw_tbl 
AS SELECT * 
FROM read_parquet('data/weca_parquet/weca_2021.parquet');
"""
con.sql(qry)

#%%

con.sql("SUMMARIZE mobile_raw_tbl;").pl().glimpse()

#%%

mob_rel = con.sql("SELECT * FROM mobile_raw_tbl;")
# %%
con.sql("SELECT DISTINCT pci_top1_3uk FROM mobile_raw_tbl;")
#%%
(mob_rel
 .select("DISTINCT pci_top1_3uk AS distinct_pci_top1_3uk")
 .filter("distinct_pci_top1_3uk IS NOT NULL")
 .show()
 )
# %%
