#%%
import py7zr


#%%
py7zr.SevenZipFile('data/os_bng_grids.7z', mode='r').extractall(path='data')
#%%

