#%%
import duckdb

#%%

con = duckdb.connect(database='data/mobile_clean.duckdb', read_only=False)


## comment
#%%
con.sql("CREATE TABLE mobile_clean AS SELECT * FROM read_parquet('data/weca_parquet/weca_mobile_clean.parquet')")
#%%
con.sql("INSTALL SPATIAL;")
con.sql("LOAD SPATIAL;")
#%%

query = """
ALTER TABLE mobile_clean 
ADD COLUMN geom GEOMETRY;

UPDATE mobile_clean 
SET geom = ST_Point(longitude, latitude);
"""

#%%
con.execute(query)
#%%

con.sql("DESCRIBE mobile_clean;")
#%%

con.sql("SELECT geom FROM mobile_clean LIMIT 5;")
#%%
con.sql("SELECT ST_AsText(geom) FROM mobile_clean LIMIT 5")
#%%
con.close()

#%%


#%%



#%%





#%%