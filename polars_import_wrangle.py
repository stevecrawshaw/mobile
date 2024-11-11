#%%
import duckdb
import polars as pl

#%%
path1 = "data/nr_2023_1.csv" #9GB
path2 = "data/nr_2023_3.csv" #24GB
#%%

raw = pl.scan_csv(path1)


#%%
%%time
out_df = (raw
 .filter(pl.col("latitude").is_between(51, 52))
#  .filter(pl.col("longitude").is_between(-3, -2))
).collect()

#%%
out_df.schema


#%%