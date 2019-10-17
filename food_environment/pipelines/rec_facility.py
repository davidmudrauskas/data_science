import os
from pyspark.sql import SparkSession, functions
from pyspark.sql.utils import AnalysisException
from pipeline_utils import get_dfs

PROJECT_DIRECTORY = '/Users/dmudrauskas/Work/data_science/food_environment'
RAW_DATA_DIRECTORY = PROJECT_DIRECTORY + '/raw_data'
PROCESSED_DATA_DIRECTORY = PROJECT_DIRECTORY + '/processed_data'

spark = SparkSession \
  .builder \
  .appName("Rec facility data") \
  .getOrCreate()

def get_rec_facility_data(yy):
  dfs = get_dfs(spark, RAW_DATA_DIRECTORY)
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

def main():
  # Restructure table into time series extended by adding rows instead of columns
  rec_facilities_2009 = get_rec_facility_data('09')
  rec_facilities_2014 = get_rec_facility_data('14')

  rec_facilities = rec_facilities_2009.union(rec_facilities_2014)
  rec_facilities.toPandas().to_csv(PROCESSED_DATA_DIRECTORY + '/rec_facilities.csv', header=True, index=False)

if __name__ == '__main__':
  main()
