import os
from pyspark.sql import functions
from pyspark.sql.utils import AnalysisException

DATA_DIRECTORY = '/Users/dmudrauskas/Work/data_science/food_environment/data'

def get_dfs():
  # Simple abstract way to read all data files
  dfs = {}
  
  filenames = os.listdir(DATA_DIRECTORY)
  
  for f in filenames:
    # Name dataframe after filename without extension
    df_name = f.lower().split('.')[0]
    data_path = '/'.join([DATA_DIRECTORY, f])
    dfs[df_name] = spark.read.csv(data_path, header=True)
  
  return dfs


def get_rec_facility_data(yy):
  health = dfs['health']
  
  try:
    return health.select(
      health.FIPS,
      health.State,
      health.County,
      functions.col('RECFAC' + yy).alias('RECFAC'),
      functions.col('RECFACPTH' + yy).alias('RECFACPTH'),
      functions.lit(int('20' + yy)).alias('YEAR')
    )
  except AnalysisException:
    print("No rec facility data for '{}".format(yy))


dfs = get_dfs()

# Restructure table into time series extended by adding rows instead columns
rec_facilities_2009 = get_rec_facility_data('09')
rec_facilities_2014 = get_rec_facility_data('14')

rec_facilities = rec_facilities_2009.union(rec_facilities_2014)
rec_facilities.toPandas().to_csv(DATA_DIRECTORY + '/rec_facilities.csv', header=True, index=False)
