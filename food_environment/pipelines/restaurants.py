import os
from pyspark.sql import SparkSession, functions
from pyspark.sql.utils import AnalysisException
from pipeline_utils import get_dfs

PROJECT_DIRECTORY = '/Users/dmudrauskas/Work/data_science/food_environment'
RAW_DATA_DIRECTORY = PROJECT_DIRECTORY + '/raw_data'
PROCESSED_DATA_DIRECTORY = PROJECT_DIRECTORY + '/processed_data'

spark = SparkSession \
  .builder \
  .appName("Restaurant data") \
  .getOrCreate()

def get_restaurant_data(yy):
  dfs = get_dfs(spark, RAW_DATA_DIRECTORY)
  restaurants = dfs['restaurants']
  
  try:
    return restaurants.select(
      restaurants.FIPS,
      restaurants.State,
      restaurants.County,
      functions.col('FFR' + yy).alias('FFR'),
      functions.col('FFRPTH' + yy).alias('FFRPTH'),
      functions.col('FSR' + yy).alias('FSR'),
      functions.col('FSRPTH' + yy).alias('FSRPTH'),
      functions.lit(int('20' + yy)).alias('YEAR')
    )
  except AnalysisException:
    print("No restaurant data for '{}".format(yy))

def main():
  # Restructure table into time series extended by adding rows instead of columns
  restaurants_2009 = get_restaurant_data('09')
  restaurants_2014 = get_restaurant_data('14')

  restaurants = restaurants_2009.union(restaurants_2014)
  restaurants.toPandas().to_csv(PROCESSED_DATA_DIRECTORY + '/restaurants.csv', header=True, index=False)

if __name__ == '__main__':
  main()
