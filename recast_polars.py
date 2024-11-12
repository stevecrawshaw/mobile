#%%
import polars as pl
import polars.selectors as cs

#%%
path = "data/weca_parquet/weca_2021.parquet"

#%%

mobile_raw_df = pl.read_parquet(path)

#%%
mobile_raw_df.glimpse()
#%%

mobile_clean_df = (mobile_raw_df
    # turn date string to date
 .with_columns([pl.col("month_year").cast(pl.Date)])

# if a column is string type and contains "-", "n.a.", "/", 
# or any capital letter, then turn it to None

.with_columns(pl.when(cs.string().str.contains("-|n\\.a\\.|/|[A-Z]"))
               .then(pl.lit(None, allow_object=True))
               .otherwise(cs.string())
               .name.keep())
# cast all string columns to float
.with_columns(cs.string().cast(pl.Float64))
 
 )
 #%%
del mobile_raw_df
#%%
mobile_clean_df.describe()

#%%
mobile_clean_df.write_parquet("data/weca_parquet/weca_mobile_clean.parquet")

#%%

#%%




#%%

#%%


#%%


#%%

#%%




#%%

