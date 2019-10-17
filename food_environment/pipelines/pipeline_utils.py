import os

def get_dfs(spark, directory):
  # Simple abstract way to read all data files
  dfs = {}
  
  filenames = os.listdir(directory)
  
  for f in filenames:
    # Name dataframe after filename without extension
    df_name = f.lower().split('.')[0]
    data_path = '/'.join([directory, f])
    dfs[df_name] = spark.read.csv(data_path, header=True)
  
  return dfs
