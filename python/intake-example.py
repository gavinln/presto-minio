import intake

# list items in catalog
print(list(intake.cat))

# read a csv file
source = intake.open_csv(
    'https://timeseries.weebly.com/uploads/2/1/0/8/21086414/sea_ice.csv')

# discover basic information about the data source
print(source.discover())

# display the number of partitions
print(source.npartitions)

# display the kind of containe the source produces
print(source.container)

# read the first partition
df = source.read_partition(0)
print(type(df))
print(df.shape)

# Create a catalog file from the source
print(source.yaml())

cat = intake.open_catalog('https://raw.githubusercontent.com/intake/'
                          'intake-examples/master/tutorial/sea.yaml')

# Create a catalog file from the catalog which acts as a source
print(cat.yaml())

# list itmes in catalog
print(list(cat))

source = cat.sea_ice

print(source.container)

# read as dask dataframe
ddf = source.to_dask()

# get size (only columns)
ddf.shape

# get rows and columns size
ddf.shape[0].compute()

# Read a catalog from the web
cat = intake.open_catalog(
    'https://raw.githubusercontent.com/intake/intake-examples/'
    'master/us_crime/us_crime.yaml')

print(list(cat))

# uses the CATALOG_DIR variable to read data
# CATALOG_DIR is set to the location of the catalog file
ddf = cat.us_crime.to_dask()

ddf.shape[0].compute()

cat = intake.open_catalog(
    'https://raw.githubusercontent.com/intake/intake-examples/'
    'master/nyc_taxi/nyc_taxi.yaml')

# check list of drivers
intake.autodiscover()

# list of sources in a catalog
print(list(cat))

# very slow as it downloads a 1.5G csv file to ~/.intake/cache
# cat.nyc_taxi_cache.discover()

# create a dask dataframe
# ddf = cat.nyc_taxi_cache.to_dask()

# displays the length of the data frame
# ddf.shape[0].compute()

cat.nyc_taxi.discover()

ddf = cat.nyc_taxi.to_dask()

ddf.shape[0].compute()

cat = intake.open_catalog(
    'https://raw.githubusercontent.com/intake/intake-examples/'
    'master/airline_flights/airline.yaml')

print(list(cat))

# downloads a 15M parq cache file to ~/.intake/cache
cat.airline_flights.discover()

ddf = cat.airline_flights.to_dask()


