Python and R scripts for ingesting and filtering mobile data coverage data from Ofcom.
The files are large zip files with csv files for each zip which are derived from driving routes and measuring mobile signal strength from various operators.
The geographic coverage for each files is not known in advance, so each file must be downloaded and filtered for membership of a bounding box for the west of england.
Two approaches were tried to handle these large files:
1. duckdb applied to the csv
2. Polars

Both of these are very fast - 15 - 40 seconds for a 24GB CSV to ingest and filter for the bounding box on a Dell laptop.
For the "production" version, polars was selected. It is slightly slower than duckdb, but the API is (opinionated) easier to work with.

All the files are downloaded and unzipped.
Each csv is scanned by polars' lazy API and filtered for the bounding box.
The filtered polars dataframe is saved to parquet. Approximately 52m rows.
The parquet files containing data are then concatenated diagonally with polars.
The multiple string columns that arise from coercion in the concatenation process are cast to float after regex replacement of certain characters with polars conditional expressions and selectors.
Finally the cleaned parquet files is ingested to a duckdb file database and a geometry column added using duckdb's spatial extension.
