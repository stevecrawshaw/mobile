#%%
import polars as pl
from pathlib import Path
#%%
path1 = "data/nr_2023_1.csv" #9GB
path2 = "data/nr_2023_3.csv" #24GB
#%%

raw = pl.scan_csv(path2)


#%%
%%time
out_df = (raw
 .filter(pl.col("latitude").is_between(51, 52))
 .filter(pl.col("longitude").is_between(-3, -1))
).collect()

#%%
out_df.write_csv("data/nr_2023_3_filtered.csv")


#%%
out_df.glimpse()
#%%

test = pl.read_parquet("data/nr_2021_1.parquet")


#%%

test.glimpse()
# %%
folder  = Path("data/weca_parquet")
parquet_files = list(folder.glob('*.parquet'))
#%%
parquet_files
# %%
pldf = [pl.read_parquet(file) for file in parquet_files ]
# %%

mobile_df = pl.concat(pldf[0:2], how="vertical_relaxed")
# %%
